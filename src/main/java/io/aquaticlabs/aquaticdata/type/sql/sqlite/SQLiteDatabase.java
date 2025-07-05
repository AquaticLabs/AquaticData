package io.aquaticlabs.aquaticdata.type.sql.sqlite;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.SerializedData;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.model.StorageValue;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.type.ColumnData;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnData;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import io.aquaticlabs.aquaticdata.type.sql.SQLDatabase;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.DataDebugLogType;
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


        for (Map.Entry<String, ColumnData<?>> entry : getTableStructure().getColumnStructure().entrySet()) {
            String sqlValueKey = entry.getKey();
            SQLColumnData<?> sqlColumnData = (SQLColumnData<?>) entry.getValue();
            String sqlValueTypeString = sqlColumnData.getColumnType().getSql();

            if (first) {
                builder
                        .append(sqlValueKey)
                        .append(" ")
                        .append(sqlValueTypeString)
                        .append(" PRIMARY KEY");
                first = false;
                continue;
            }
            builder
                    .append(", ")
                    .append(sqlValueKey)
                    .append(" ")
                    .append(sqlValueTypeString)
                    .append(" NOT NULL");
        }
        builder.append(") ");
        DataDebugLog.logDebug(DataDebugLogType.SQL_QUERIES, builder.toString());

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
    protected void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnData<?>>> addColumns) {
        long loadStart = System.currentTimeMillis();

        String tempTableName = "FCtempTable";
        String dropConflict = "DROP TABLE IF EXISTS " + tempTableName + ";";

        try (PreparedStatement dropTempTable = connection.prepareStatement(dropConflict)) {
            dropTempTable.executeUpdate();
        } catch (SQLException ex) {
            DataDebugLog.logDebug(DataDebugLogType.SQL_EXCEPTIONS, "Failed to drop temp table: " + ex.getMessage());
        }

        // Rename original table
        String renameTableStmt = "ALTER TABLE " + getCredential().getTableName() + " RENAME TO " + tempTableName + ";";
        try (PreparedStatement renameTable = connection.prepareStatement(renameTableStmt)) {
            renameTable.executeUpdate();
            DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, "Table renamed to: " + tempTableName);
        } catch (SQLException ex) {
            DataDebugLog.logDebug(DataDebugLogType.SQL_EXCEPTIONS, "Failed to rename table: " + ex.getMessage());
            return;
        }

        // Create the new table
        try {
            connection.createStatement().executeUpdate(createTableStatement(true));
        } catch (SQLException ex) {
            DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, "Failed to create new table: " + ex.getMessage());
            return;
        }
        String targetTable = getCredential().getTableName();

        try {
            connection.setAutoCommit(false); // Start a transaction

            List<String> matchingColumns = getMatchingColumns(connection, tempTableName, targetTable);

            if (matchingColumns.isEmpty()) {
                DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, "No matching columns found between the tables.");
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
            DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, "Dropping table: " + tempTableName);
        } catch (Exception ex) {
            DataDebugLog.logDebug(DataDebugLogType.SQL_EXCEPTIONS, "Failed to Drop temp Table. " + ex.getMessage());
        }

        long loadEnd = System.currentTimeMillis();
        long loadElapsedTime = loadEnd - loadStart;
        DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, "Data Conversion Loading time: " + loadElapsedTime + "ms");
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
                SQLColumnData<?> columnData = (SQLColumnData<?>) getTableStructure().getColumnStructure().get(targetColumn);
                Object defaultValue = (columnData != null) ? columnData.getDefaultValue() : "";


                selectClause.append("'").append(defaultValue).append("' AS ").append(targetColumn);
            }

            insertColumns.append(targetColumn);
        }

        // Build the SQL query
        String copyQuery = String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s",
                targetTable, insertColumns, selectClause, sourceTable
        );

        DataDebugLog.logDebug(DataDebugLogType.SQL_QUERIES, copyQuery);

        // Execute the query
        try (Statement stmt = connection.createStatement()) {
            int rowsCopied = stmt.executeUpdate(copyQuery);
            DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, "Copied " + rowsCopied + " rows from " + sourceTable + " to " + targetTable);
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
        for (Map.Entry<String, ColumnData<?>> entry : modifiedStructure.getColumnStructure().entrySet()) {
            Object value = entry.getValue().getValueOrDefault();
            SQLColumnData<?> sqlColumnData = (SQLColumnData<?>) entry.getValue();
            String valueString = sqlColumnData.getColumnType().needsQuotes() ? "'" + value.toString() + "'" : value.toString();
            if (first) {
                // skip comma
                builder.append(valueString);
                first = false;
                continue;
            }
            builder.append(", ").append(valueString);
        }
        builder.append(")");

        DataDebugLog.logDebug(DataDebugLogType.SQL_QUERIES, builder.toString());
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
        for (Map.Entry<String, ColumnData<?>> entry : modifiedStructure.getColumnStructure().entrySet()) {
            SQLColumnData<?> sqlColumnData = (SQLColumnData<?>) entry.getValue();

            if (first) {
                // skip key
                first = false;
                continue;
            }
            builder
                    .append(entry.getKey())
                    .append(" = ")
                    .append(sqlColumnData.getColumnType().needsQuotes() ? "'" + entry.getValue().getValueOrDefault().toString() + "'" : entry.getValue().getValueOrDefault().toString());
            builder.append(", ");
        }
        builder.deleteCharAt(builder.toString().length() - 2);
        Map.Entry<String, ColumnData<?>> entry = modifiedStructure.getColumnStructure().entrySet().iterator().next();
        String key = entry.getKey();
        Object value = entry.getValue().getValueOrDefault();
        builder.append("WHERE ")
                .append(key)
                .append(" = '")
                .append(value)
                .append("';");

        DataDebugLog.logDebug(DataDebugLogType.SQL_QUERIES, builder.toString());

        return builder.toString();
    }

    @Override
    public void dropTable() {
        executeRequest(new ConnectionRequest<>(connection -> {
            String dropConflict = "DROP TABLE IF EXISTS " + getCredential().getTableName() + ";";
            try (PreparedStatement preparedStatement = connection.prepareStatement(dropConflict)) {
                preparedStatement.executeUpdate();
            } catch (Exception ex) {
                DataDebugLog.logDebug(DataDebugLogType.SQL_EXCEPTIONS, "Sqlite Failed to drop if exists Table. " + ex.getMessage());
            }
            return null;
        }, getSyncExecutor()));
    }

}
