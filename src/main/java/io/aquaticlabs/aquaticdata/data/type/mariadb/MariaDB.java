package io.aquaticlabs.aquaticdata.data.type.mariadb;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.aquaticlabs.aquaticdata.AquaticDatabase;
import io.aquaticlabs.aquaticdata.data.HikariCPDatabase;
import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.data.storage.ColumnType;
import io.aquaticlabs.aquaticdata.data.storage.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.data.type.DataCredential;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;

import java.util.List;

/**
 * @Author: extremesnow
 * On: 8/21/2022
 * At: 23:29
 */
public class MariaDB extends HikariCPDatabase {

    private final DataCredential dataCredential;
    private final DataObject object;

    public MariaDB(DataCredential dataCredential, DataObject object) {
        super(dataCredential.getTableName());
        this.dataCredential = dataCredential;
        this.object = object;

        HikariConfig config = new HikariConfig();
        config.setPoolName("Aquatic Labs MariaDB Pool");
        config.setDriverClassName("org.mariadb.jdbc.Driver");

        String url = "jdbc:mariadb://" + dataCredential.getHostname() + ":" + dataCredential.getPort() + "/" + dataCredential.getDatabaseName();
        url += "?allowPublicKeyRetrieval=" + dataCredential.isAllowPublicKeyRetrieval() + "&useSSL=" + dataCredential.isUseSSL() + "&characterEncoding=utf8";

        config.setJdbcUrl(url);

        config.setUsername(dataCredential.getUsername());
        config.setPassword(dataCredential.getPassword());

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

        verifyTable(object.getStructure());

    }

    @Override
    public String insertStatement(List<DataEntry<String, String>> columns) {
        List<DataEntry<String, ColumnType>> structure = object.getStructure();

        StringBuilder builder = new StringBuilder();
        builder
                .append("INSERT INTO ")
                .append(dataCredential.getTableName())
                .append(" (");

        int i = 0;
        for (DataEntry<String, String> column : columns) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(column.getKey());
            i++;
        }
        builder.append(") VALUES (");

        i = 0;
        for (DataEntry<String, String> column : columns) {
            if (column.getValue() == null) continue; //fix this?
            if (i > 0) {
                builder.append(", ");
            }

            if (structure.get(i).getValue().needsQuotes()) {
                builder.append("'").append(column.getValue().replace("'", "")).append("'");
            } else {
                builder.append(column.getValue());
            }
            i++;
        }


        builder.append(");");

        DataDebugLog.logDebug(builder.toString());
        return builder.toString();
    }
    // INSERT INTO TABLE (rowPK, row2, row3) VALUES (primaryKey, value2, value3);



    @Override
    public String buildUpdateStatementSQL(List<DataEntry<String, String>> columns) {

        DataEntry<String, String> key = columns.get(0);
        List<DataEntry<String, ColumnType>> structure = object.getStructure();

        StringBuilder builder = new StringBuilder();
        builder
                .append("UPDATE ")
                .append(dataCredential.getTableName())
                .append(" SET ");

        for (int i = 1 ; i < columns.size(); i++) {
            DataEntry<String, String> updatedData = columns.get(i);

            ColumnType columnType = structure.stream().filter(entry -> entry.getKey().equalsIgnoreCase(updatedData.getKey())).map(DataEntry::getValue).findFirst().orElse(null);
            if (columnType == null) continue;

            builder.append(updatedData.getKey())
                    .append(" = ");
            if (columnType.needsQuotes()) {
                builder.append("'").append(updatedData.getValue().replace("'", "")).append("'");
            } else {
                builder.append(updatedData.getValue());
            }
            if (i < columns.size() - 1) {
                builder.append(", ");
            }
        }

        builder.append(" WHERE ")
                .append(key.getKey())
                .append(" = '")
                .append(key.getValue())
                .append("';");

        DataDebugLog.logDebug("MARIADB UPDATE STATEMENT: " + builder.toString());

        return builder.toString();
    }


    private void verifyTable (List<DataEntry<String, ColumnType>> columns) {
        createTable(columns, false);
    }


    private String buildCreateTableSQL(List<DataEntry<String, ColumnType>> columns, boolean force) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE ");
        if (!force) {
            queryBuilder.append("IF NOT EXISTS ");
        }
        queryBuilder
                .append(dataCredential.getTableName())
                .append(" (");

        for (int i = 0; i < columns.size(); i++) {
            DataEntry<String, ColumnType> column = columns.get(i);
            queryBuilder.append(column.getKey()).append(" ").append(column.getValue().getSql()); // maybe add not null?
            if (i != columns.size() - 1) {
                queryBuilder.append(", ");
            }
        }
        queryBuilder
                .append(", PRIMARY KEY ( ")
                .append(columns.get(0).getKey())
                .append(" ));");

        DataDebugLog.logDebug("MARIADB TABLE CREATION: " + queryBuilder.toString());

        return queryBuilder.toString();
    }


    @Override
    public void createTable(List<DataEntry<String, ColumnType>> columns, boolean force) {
        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(conn -> {
            conn.createStatement().executeUpdate(buildCreateTableSQL(columns, force));
            return null;
        }, AquaticDatabase.getInstance().getRunner(false)));
    }
}

