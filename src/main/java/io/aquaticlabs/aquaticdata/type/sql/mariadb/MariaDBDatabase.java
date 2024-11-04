package io.aquaticlabs.aquaticdata.type.sql.mariadb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import io.aquaticlabs.aquaticdata.type.sql.SQLDatabase;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.StorageUtil;
import lombok.NonNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 18:42
 */
public class MariaDBDatabase<T extends StorageModel> extends SQLDatabase<T> {

    public MariaDBDatabase(MariaDBCredential credential, DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        super(credential, tableStructure, serializer, asyncExecutor, syncExecutor);

        HikariConfig config = new HikariConfig();
        config.setPoolName("Aquatic Labs MariaDB Pool");
        config.setDriverClassName("org.mariadb.jdbc.Driver");

        String url = "jdbc:mariadb://" + credential.getHostname() + ":" + credential.getPort() + "/" + credential.getDatabaseName();
        url += "?allowPublicKeyRetrieval=" + credential.isAllowPublicKeyRetrieval() + "&useSSL=" + credential.isUseSSL() + "&characterEncoding=utf8";

        config.setJdbcUrl(url);

        config.setUsername(credential.getUsername());
        config.setPassword(credential.getPassword());

        config.addDataSourceProperty("cachePrepStmts", true);
        config.addDataSourceProperty("prepStmtCacheSize", 250);
        config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
        config.addDataSourceProperty("useServerPrepStmts", true);
        config.addDataSourceProperty("useLocalSessionState", true);
        config.addDataSourceProperty("rewriteBatchedStatements", true);
        config.addDataSourceProperty("cacheResultSetMetadata", true);
        config.addDataSourceProperty("cacheServerConfiguration", true);
        config.addDataSourceProperty("elideSetAutoCommits", true);
        config.addDataSourceProperty("maintainTimeStats", false);
        config.addDataSourceProperty("alwaysSendSetIsolation", false);
        config.addDataSourceProperty("cacheCallableStmts", true);
        config.setConnectionTimeout(120000);
        config.setMaximumPoolSize(10);

        setHikariDataSource(new HikariDataSource(config));
    }

    @Override
    public String createTableStatement(boolean force) {

        // CREATE TABLE IF NOT EXISTS testtable (uuid VARCHAR(36), name VARCHAR(255), level INT, stat1 INT, stat2 INT, stat3 INT, PRIMARY KEY ( uuid ));
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ");
        if (!force) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(getCredential().getTableName())
                .append(" (");

        boolean first = true;
        String primaryKeyColumn = "";
        for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
            if (first) {
                primaryKeyColumn = entry.getKey();
                builder
                        .append(primaryKeyColumn)
                        .append(" ")
                        .append(entry.getValue().getSql());
                first = false;
                continue;
            }
            builder
                    .append(", ")
                    .append(entry.getKey())
                    .append(" ")
                    .append(entry.getValue().getSql());
        }
        builder.append(", PRIMARY KEY ( ")
                .append(primaryKeyColumn)
                .append(" ));");
        DataDebugLog.logDebug(builder.toString());

        return builder.toString();
    }

    @Override
    protected void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnType>> addColumns) {
        List<String> batches = new ArrayList<>();


        String alterStmt = "ALTER TABLE " + getCredential().getTableName();

        // First up: removes

        for (String col : removeColumns) {
            batches.add(alterStmt + " DROP COLUMN " + col + ";");
        }

        // Next is adds

        for (Map.Entry<String, Map.Entry<String, SQLColumnType>> columnEntry : addColumns.entrySet()) {
            batches.add(alterStmt + " ADD " + columnEntry.getValue().getKey() + " " + columnEntry.getValue().getValue().getSql() + " NOT NULL AFTER " + columnEntry.getKey() + ";");
        }

        // Now moves

        for (Map.Entry<String, String> moveEntry : moveColumns.entrySet()) {
            batches.add(alterStmt + " CHANGE " + moveEntry.getKey() + " " + moveEntry.getKey() + " " + getTableStructure().getColumnStructure().get(moveEntry.getKey()).getSql() + " NOT NULL AFTER " + moveEntry.getValue() + ";");
        }

        // Lastly Type Changes
        for (Map.Entry<String, SQLColumnType> retypeEntry : retypeColumns.entrySet()) {
            batches.add(alterStmt + " MODIFY COLUMN `" + retypeEntry.getKey() + "` " + retypeEntry.getValue().getSql() + " NOT NULL DEFAULT '0';");
        }

        try (Statement statement = connection.createStatement()) {

            for (String batch : batches) {
                statement.addBatch(batch);
            }
            statement.executeBatch();
            DataDebugLog.logDebug("Success executing alter table batches.");

        } catch (Exception ex) {
            DataDebugLog.logDebug("Failed to Alter Table. " + ex.getMessage());
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
    public String insertPreparedStatement(DatabaseStructure modifiedStructure) {
        return null;
    }

    @Override
    public String updatePreparedStatement(DatabaseStructure modifiedStructure) {
        return null;
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
