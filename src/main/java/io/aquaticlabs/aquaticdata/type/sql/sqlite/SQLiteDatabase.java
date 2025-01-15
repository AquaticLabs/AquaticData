package io.aquaticlabs.aquaticdata.type.sql.sqlite;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.SerializedData;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.model.StorageValue;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import io.aquaticlabs.aquaticdata.type.sql.SQLDatabase;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.StorageUtil;
import lombok.NonNull;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 18:42
 */
public class SQLiteDatabase<T extends StorageModel> extends SQLDatabase<T> {

    public SQLiteDatabase(SQLiteCredential credential, DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        super(credential, tableStructure, serializer, asyncExecutor, syncExecutor);
        HikariConfig config = new HikariConfig();
        config.setPoolName("Aquatic Labs SQLite Pool");
        config.setDriverClassName("org.sqlite.JDBC");
        config.setJdbcUrl("jdbc:sqlite:" + credential.getFolder().getAbsolutePath() + File.separator + credential.getDatabaseName() + ".db");
        config.setConnectionTestQuery("SELECT 1");

        config.setMaxLifetime(60000); // 60 Sec
        config.setMaximumPoolSize(40);
        config.setConnectionTimeout(120000); // 120 sec

        setHikariDataSource(new HikariDataSource(config));
    }

    @Override
    public String createTableStatement(boolean force) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ");
        if (!force) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(getCredential().getTableName())
                .append(" (");

        boolean first = true;
        for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
            if (first) {
                builder
                        .append(entry.getKey())
                        .append(" ")
                        .append(entry.getValue().getSql())
                        .append(" PRIMARY KEY");
                first = false;
                continue;
            }
            builder
                    .append(", ")
                    .append(entry.getKey())
                    .append(" ")
                    .append(entry.getValue().getSql())
                    .append(" NOT NULL");
        }
        builder.append(") ");
        DataDebugLog.logDebug(builder.toString());

        return builder.toString();
    }

    private void setPreparedStatementValues(ResultSet rs, PreparedStatement ps, Map<String, SQLColumnType> columnStructure) throws SQLException {
        int index = 1;
        for (String column : columnStructure.keySet()) {
            Object value = rs.getObject(column);
            ps.setObject(index++, value);
        }
    }

    @Override
    protected void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnType>> addColumns) {
        long loadStart = System.currentTimeMillis();

        String tempTableName = "FCtempTable";
        String dropConflict = "DROP TABLE IF EXISTS " + tempTableName + ";";

        try (PreparedStatement dropTempTable = connection.prepareStatement(dropConflict)) {
            dropTempTable.executeUpdate();
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Failed to drop temp table: " + ex.getMessage());
        }

        // Rename original table
        String renameTableStmt = "ALTER TABLE " + getCredential().getTableName() + " RENAME TO " + tempTableName + ";";
        try (PreparedStatement renameTable = connection.prepareStatement(renameTableStmt)) {
            renameTable.executeUpdate();
            DataDebugLog.logDebug("Table renamed to: " + tempTableName);
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Failed to rename table: " + ex.getMessage());
            return;
        }

        // Create the new table
        try {
            connection.createStatement().executeUpdate(createTableStatement(true));
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Failed to create new table: " + ex.getMessage());
            return;
        }
        String targetTable = getCredential().getTableName();

        try {
            connection.setAutoCommit(false); // Start a transaction

            List<String> matchingColumns = getMatchingColumns(connection, tempTableName, targetTable);

            if (matchingColumns.isEmpty()) {
                System.out.println("No matching columns found between the tables.");
                return;
            }
            copyData(connection, tempTableName, targetTable);
            connection.commit(); // Commit the transaction
        } catch (SQLException e) {
            DataDebugLog.logError("Failed column correcting " + getCredential().getTableName() + ": " + e.getMessage());
            try {
                connection.rollback();
            } catch (SQLException ex) {
                DataDebugLog.logError("Failed to rollback batch insert on column correcting: " + ex.getMessage());
            }
        }

        String dropStmt = "DROP TABLE '" + tempTableName + "'";
        try (PreparedStatement dropStatement = connection.prepareStatement(dropStmt)) {
            dropStatement.executeUpdate();
            DataDebugLog.logDebug("Dropping table: " + tempTableName);
        } catch (Exception ex) {
            DataDebugLog.logDebug("Failed to Drop temp Table. " + ex.getMessage());
        }

        long loadEnd = System.currentTimeMillis();
        long loadElapsedTime = loadEnd - loadStart;
        System.out.println("Data Conversion Loading time: " + loadElapsedTime + "ms");
        //DataDebugLog.logDebug("Data Conversion Loading time: " + loadElapsedTime + "ms");
    }

    private void copyData(Connection connection, String sourceTable, String targetTable) throws SQLException {
        // Fetch column names for both source and target tables
        List<String> sourceColumns = getTableColumns(connection, sourceTable);
        List<String> targetColumns = getTableColumns(connection, targetTable);

        // Prepare the SELECT clause
        StringBuilder selectClause = new StringBuilder();
        StringBuilder insertColumns = new StringBuilder();

        for (String targetColumn : targetColumns) {
            if (insertColumns.length() > 0) {
                insertColumns.append(", ");
                selectClause.append(", ");
            }

            if (sourceColumns.contains(targetColumn)) {
                // If the column exists in the source table, use it
                selectClause.append(targetColumn);
            } else {
                // Otherwise, use the default value
                Object defaultValue = getTableStructure().getColumnDefaults().getOrDefault(targetColumn, "");
                selectClause.append("'").append(defaultValue).append("' AS ").append(targetColumn);
            }

            insertColumns.append(targetColumn);
        }

        // Build the SQL query
        String copyQuery = String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s",
                targetTable, insertColumns, selectClause, sourceTable
        );

        System.out.println(copyQuery);

        // Execute the query
        try (Statement stmt = connection.createStatement()) {
            int rowsCopied = stmt.executeUpdate(copyQuery);
            System.out.println("Copied " + rowsCopied + " rows from " + sourceTable + " to " + targetTable);
        }
    }

    private static List<String> getTableColumns(Connection connection, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }
/*
    private void copyData(Connection connection, String sourceTable, String targetTable, List<String> matchingColumns) throws SQLException {
        String columnList = String.join(", ", matchingColumns);

        String copyQuery = String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s",
                targetTable, columnList, columnList, sourceTable
        );
        System.out.println(copyQuery);

        try (Statement stmt = connection.createStatement()) {
            int rowsCopied = stmt.executeUpdate(copyQuery);
            System.out.println("Copied " + rowsCopied + " rows from " + sourceTable + " to " + targetTable);
        }
    }
*/

    private List<String> getMatchingColumns(Connection connection, String sourceTable, String targetTable) throws SQLException {
        List<String> sourceColumns = getTableColumns(connection, sourceTable);
        List<String> targetColumns = getTableColumns(connection, targetTable);

        // Find matching columns
        List<String> matchingColumns = new ArrayList<>();
        for (String column : sourceColumns) {
            if (targetColumns.contains(column)) {
                matchingColumns.add(column);
            }
        }
        return matchingColumns;
    }
/*
    @Override
    protected void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnType>> addColumns) {
        long loadStart = System.currentTimeMillis();

        String tempTableName = "FCtempTable";
        String dropConflict = "DROP TABLE IF EXISTS " + tempTableName + ";";

        try (PreparedStatement dropTempTable = connection.prepareStatement(dropConflict)) {
            dropTempTable.executeUpdate();
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Failed to drop temp table: " + ex.getMessage());
        }

        // Rename original table
        String renameTableStmt = "ALTER TABLE " + getCredential().getTableName() + " RENAME TO " + tempTableName + ";";
        try (PreparedStatement renameTable = connection.prepareStatement(renameTableStmt)) {
            renameTable.executeUpdate();
            DataDebugLog.logDebug("Table renamed to: " + tempTableName);
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Failed to rename table: " + ex.getMessage());
            return;
        }

        // Create the new table
        try {
            connection.createStatement().executeUpdate(createTableStatement(true));
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Failed to create new table: " + ex.getMessage());
            return;
        }
        String selectQuery2 = "INSERT INTO " + getTableStructure().getTableName() + " ";

        try (Statement selectStatement = connection.createStatement();
             ResultSet rs = selectStatement.executeQuery(selectQuery2)) {
        } catch (SQLException e) {
        }


        // Migrate data from temp table to new table
        String selectQuery = "SELECT * FROM " + tempTableName;
        try (Statement selectStatement = connection.createStatement();
             ResultSet rs = selectStatement.executeQuery(selectQuery)) {

            connection.setAutoCommit(false); // Start transaction

            String insertSQL = buildInsertStatementTemplate(getTableStructure().getColumnStructure());
            try (PreparedStatement insertStatement = connection.prepareStatement(insertSQL)) {
                int batchSize = getBatchSize();
                int count = 0;

                while (rs.next()) {
                    setPreparedStatementValues(rs, insertStatement, getTableStructure().getColumnStructure());
                    insertStatement.addBatch();
                    count++;

                    if (count % batchSize == 0) {
                        executeBatchSafely(insertStatement, count);
                    }
                }

                // Execute remaining batch
                executeBatchSafely(insertStatement, count);

                connection.commit(); // Commit transaction
                DataDebugLog.logDebug("Data migration completed successfully.");
            } catch (SQLException ex) {
                DataDebugLog.logError("Error during batch execution: " + ex.getMessage());
                connection.rollback();
                throw ex; // Rethrow to ensure proper handling
            }

        } catch (SQLException ex) {
            DataDebugLog.logError("Error during column correction: " + ex.getMessage());
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                DataDebugLog.logError("Failed to rollback transaction: " + rollbackEx.getMessage());
            }
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException ex) {
                DataDebugLog.logError("Failed to enable AutoCommit: " + ex.getMessage());
            }
        }

        // Drop temporary table
        String dropTempStmt = "DROP TABLE " + tempTableName;
        try (PreparedStatement dropStatement = connection.prepareStatement(dropTempStmt)) {
            dropStatement.executeUpdate();
            DataDebugLog.logDebug("Temporary table dropped: " + tempTableName);
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Failed to drop temporary table: " + ex.getMessage());
        }
        long loadEnd = System.currentTimeMillis();
        long loadElapsedTime = loadEnd - loadStart;
        DataDebugLog.logDebug("Data Conversion Loading time: " + loadElapsedTime + "ms");
    }


    private String buildInsertStatementTemplate(Map<String, SQLColumnType> columnStructure) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(getCredential().getTableName()).append(" (");
        StringBuilder valuesPlaceholder = new StringBuilder(" VALUES (");

        for (String column : columnStructure.keySet()) {
            sb.append(column).append(",");
            valuesPlaceholder.append("?,");
        }

        // Remove trailing commas
        sb.setLength(sb.length() - 1);
        valuesPlaceholder.setLength(valuesPlaceholder.length() - 1);

        sb.append(")").append(valuesPlaceholder).append(")");
        return sb.toString();
    }
*/

/*

    @Override
    protected void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnType>> addColumns) {

        String tempTableName = "FCtempTable";
        String dropConflict = "DROP TABLE IF EXISTS " + tempTableName + ";";

        try (PreparedStatement preparedStatement = connection.prepareStatement(dropConflict)) {
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Failed to drop if exists Table. " + ex.getMessage());
        }


        String stmt1 = "ALTER TABLE " + getCredential().getTableName() + " RENAME TO " + tempTableName + ";";
        DataDebugLog.logDebug(stmt1);
        try (PreparedStatement preparedStatement = connection.prepareStatement(stmt1)) {
            preparedStatement.executeUpdate();
            DataDebugLog.logDebug("Success renaming table.");

        } catch (Exception ex) {
            DataDebugLog.logDebug("Failed to Alter Table. " + ex.getMessage());
            return;
        }

        try {
            connection.createStatement().executeUpdate(createTableStatement(true));
        } catch (SQLException e) {
            DataDebugLog.logDebug("Failed to create new table. " + e.getMessage());
        }

        try (ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM " + tempTableName)) {
            connection.setAutoCommit(false); // Start a transaction

            Statement statement = connection.createStatement();

            int count = 0;
            while (rs.next()) {
                List<StorageValue> data = new LinkedList<>();

                for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
                    try {
                        DatabaseMetaData metaData = connection.getMetaData();
                        ResultSet colRs = metaData.getColumns(null, null, tempTableName, entry.getKey());
                        if (colRs.next()) {
                            data.add(new StorageValue(entry.getKey(), rs.getObject(entry.getKey()), getTableStructure().getColumnStructure().get(entry.getKey())));
                        }
                    } catch (Exception ex) {
                        data.add(new StorageValue(entry.getKey(), getTableStructure().getColumnDefaults().get(entry.getKey()), getTableStructure().getColumnStructure().get(entry.getKey())));
                    }
                }

                SerializedData serializedData = new SerializedData();
                serializedData.fromQuery(data);
                getSerializer().deserialize(null, serializedData);

                String insertStatement = insertStatement(serializedData.toDatabaseStructure(getTableStructure()));
                statement.addBatch(insertStatement);
                count++;

                if (count % getBatchSize() == 0) {
                    try {
                        statement.executeBatch();
                        statement.clearBatch();

                        DataDebugLog.logDebug("Success executing batch of " + count);

                    } catch (SQLException e) {
                        DataDebugLog.logDebug("Failed executing batch: " + e.getMessage());
                    }
                }
            }
            DataDebugLog.logDebug("Executing Last batch of " + count);

            statement.executeBatch();
            connection.commit(); // Commit the transaction

        } catch (SQLException e) {
            DataDebugLog.logError("Failed column correcting " + getCredential().getTableName() + ": " + e.getMessage());
            try {
                connection.rollback();
            } catch (SQLException ex) {
                DataDebugLog.logError("Failed to rollback batch insert on column correcting: " + ex.getMessage());
            }
        }
        try {
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            DataDebugLog.logError("Failed to enable AutoCommit: " + e.getMessage());
        }

        String dropStmt = "DROP TABLE '" + tempTableName + "'";
        try (PreparedStatement dropStatement = connection.prepareStatement(dropStmt)) {
            dropStatement.executeUpdate();
            DataDebugLog.logDebug("Dropping table: " + tempTableName);
        } catch (Exception ex) {
            DataDebugLog.logDebug("Failed to Drop temp Table. " + ex.getMessage());
        }
    }
*/

    @Override
    public String insertStatement(DatabaseStructure modifiedStructure) {

        // Should look like:
        // INSERT INTO TABLE (rowPK, row2, row3) VALUES (primaryKey, value2, value3);

        StringBuilder builder = new StringBuilder();
        builder
                .append("INSERT INTO ")
                .append(getCredential().getTableName())
                .append(" (")
                .append(String.join(", ", getTableStructure().getColumnStructure().keySet()))
                .append(") VALUES (");
        boolean first = true;
        for (Map.Entry<String, Object> entry : modifiedStructure.getColumnValues().entrySet()) {
            Object value = StorageUtil.isAtDefaultValue(entry.getValue()) ? getTableStructure().getColumnDefaults().get(entry.getKey()) : entry.getValue();
            String valueString = modifiedStructure.getColumnStructure().get(entry.getKey()).needsQuotes() ? "'" + value.toString() + "'" : value.toString();
            if (first) {
                // skip comma
                builder.append(valueString);
                first = false;
                continue;
            }
            builder.append(", ").append(valueString);
        }
        builder.append(")");

        DataDebugLog.logDebug(builder.toString());
        return builder.toString();
    }

    @Override
    public String updateStatement(DatabaseStructure modifiedStructure) {
        StringBuilder builder = new StringBuilder();
        builder
                .append("UPDATE ")
                .append(getCredential().getTableName())
                .append(" SET ");

        boolean first = true;
        for (Map.Entry<String, Object> entry : modifiedStructure.getColumnValues().entrySet()) {
            if (first) {
                // skip key
                first = false;
                continue;
            }
            builder.append(entry.getKey()).append(" = ").append(modifiedStructure.getColumnStructure().get(entry.getKey()).needsQuotes() ? "'" + entry.getValue().toString() + "'" : entry.getValue().toString());
            builder.append(", ");
        }
        builder.deleteCharAt(builder.toString().length() - 2);
        Map.Entry<String, Object> entry = modifiedStructure.getColumnValues().entrySet().iterator().next();
        String key = entry.getKey();
        Object value = entry.getValue();
        builder.append("WHERE ")
                .append(key)
                .append(" = '")
                .append(value)
                .append("';");

        DataDebugLog.logDebug(builder.toString());

        return builder.toString();
    }

    @Override
    public void dropTable() {
        executeRequest(new ConnectionRequest<>(connection -> {
            String dropConflict = "DROP TABLE IF EXISTS " + getCredential().getTableName() + ";";
            try (PreparedStatement preparedStatement = connection.prepareStatement(dropConflict)) {
                preparedStatement.executeUpdate();
            } catch (Exception ex) {
                DataDebugLog.logDebug("Sqlite Failed to drop if exists Table. " + ex.getMessage());
            }
            return null;
        }, getSyncExecutor()));
    }

}
