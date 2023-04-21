package io.aquaticlabs.aquaticdata.data.type.sqlite;

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

import java.io.File;
import java.util.List;

/**
 * @Author: extremesnow
 * On: 8/21/2022
 * At: 23:29
 */
public class SQLiteDB extends HikariCPDatabase {

    private final DataCredential dataCredential;
    private final DataObject object;

    public SQLiteDB(DataCredential dataCredential, DataObject object) {
        super(dataCredential.getTableName());
        this.dataCredential = dataCredential;
        this.object = object;

        HikariConfig config = new HikariConfig();
        config.setPoolName("Aquatic Labs Sqlite Pool");
        config.setDriverClassName("org.sqlite.JDBC");
        config.setJdbcUrl("jdbc:sqlite:" + dataCredential.getFolder().getAbsolutePath() + File.separator + dataCredential.getDatabaseName() + ".db");
        config.setConnectionTestQuery("SELECT 1");

        config.setMaxLifetime(60000); // 60 Sec
        config.setMaximumPoolSize(40);
        config.setConnectionTimeout(120000); // 120 sec


        setHikariDataSource(new HikariDataSource(config));

        verifyTable(object.getStructure());

    }

    @Override
    public String insertStatement(List<DataEntry<String, String>> columns) {
        List<DataEntry<String, ColumnType>> col = object.getStructure();


        StringBuilder builder = new StringBuilder();
        builder
                .append("INSERT INTO ")
                .append(dataCredential.getTableName())
                .append(" (");

        boolean first = true;
        for (DataEntry<String, String> column : columns) {
            if (!first) {
                builder.append(", ");
            } else first = false;

            builder.append(column.getKey());
        }
        builder.append(") VALUES (");

        int i = 0;

        first = true;
        for (DataEntry<String, String> column : columns) {

            if (!first) {
                builder.append(", ");
            } else first = false;

            if (col.get(i).getValue().needsQuotes()) {
                builder.append("'").append(column.getValue().replace("'", "")).append("'");
                i++;
                continue;
            }

            builder.append(column.getValue());
            i++;
        }

        builder.append(");");

        DataDebugLog.logDebug(builder.toString());
        // INSERT INTO TABLE (rowPK, row2, row3) VALUES (primaryKey, value2, value3);

        return builder.toString();
    }

    @Override
    public String buildUpdateStatementSQL(List<DataEntry<String, String>> columns) {

        DataEntry<String, String> key = columns.get(0);
        List<DataEntry<String, ColumnType>> structure = object.getStructure();

        StringBuilder builder = new StringBuilder();
        builder
                .append("UPDATE ")
                .append(dataCredential.getTableName())
                .append(" SET ");

        for (int i = 1; i < columns.size(); i++) {
            DataEntry<String, String> column = columns.get(i);
            builder.append(column.getKey())
                    .append(" = ");
            if (structure.get(i).getValue().isVarchar()) {
                builder.append("'").append(column.getValue().replace("'", "")).append("'");
            } else {
                builder.append(column.getValue());
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

        DataDebugLog.logDebug(builder.toString());

        // UPDATE TABLE SET row=value, row2=value, row3=value WHERE primaryKeyColumn='primaryKey';
        return builder.toString();
    }


    private void verifyTable(List<DataEntry<String, ColumnType>> columns) {
        createTable(columns, false);
    }

    private String buildCreateTableSQL(List<DataEntry<String, ColumnType>> columns, boolean force) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder
                .append("CREATE TABLE ");
        if (!force) {
            queryBuilder.append("IF NOT EXISTS ");
        }

        queryBuilder
                .append(dataCredential.getTableName())
                .append(" (")
                .append(columns.get(0).getKey())
                .append(" ")
                .append(columns.get(0).getValue().getSql())
                .append(" PRIMARY KEY");

        for (int i = 1; i < columns.size(); i++) {
            queryBuilder
                    .append(", ")
                    .append(columns.get(i).getKey())
                    .append(" ")
                    .append(columns.get(i).getValue().getSql())
                    .append(" NOT NULL");
        }
        queryBuilder.append(") ");
        DataDebugLog.logDebug("SQLITE TABLE CREATION: " + queryBuilder.toString());
        return queryBuilder.toString();
    }

    @Override
    public void createTable(List<DataEntry<String, ColumnType>> columns, boolean force) {
        executeNonLockConnection(new ConnectionRequest<>(conn -> {
            conn.createStatement().executeUpdate(buildCreateTableSQL(columns, force));
            return null;
        }, AquaticDatabase.getInstance().getRunner(false)));
    }
}
