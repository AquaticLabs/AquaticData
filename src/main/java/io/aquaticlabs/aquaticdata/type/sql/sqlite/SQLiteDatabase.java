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
        config.setPoolName("Frosty Core SQLite Pool");
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


    @Override
    protected void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnType>> addColumns) {
        // otherwise annoyingly annoying table copying

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
    public String insertPreparedStatement(DatabaseStructure tableStructure) {

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
        for (Map.Entry<String, Object> entry : tableStructure.getColumnValues().entrySet()) {
            if (first) {
                // skip comma
                builder.append("?");
                first = false;
                continue;
            }
            builder.append(", ").append("?");
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
    public String updatePreparedStatement(DatabaseStructure tableStructure) {
        StringBuilder builder = new StringBuilder();
        builder
                .append("UPDATE ")
                .append(getCredential().getTableName())
                .append(" SET ");

        boolean first = true;
        for (Map.Entry<String, Object> entry : tableStructure.getColumnValues().entrySet()) {
            if (first) {
                // skip key
                first = false;
                continue;
            }
            builder.append(entry.getKey()).append(" = ").append("?");
            builder.append(", ");
        }
        builder.deleteCharAt(builder.toString().length() - 2);
        Map.Entry<String, Object> entry = tableStructure.getColumnValues().entrySet().iterator().next();
        String key = entry.getKey();
        builder.append("WHERE ")
                .append(key)
                .append(" = ")
                .append("?")
                .append(";");

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
