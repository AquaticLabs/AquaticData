package io.aquaticlabs.aquaticdata.data;

import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.data.storage.ColumnType;
import io.aquaticlabs.aquaticdata.data.storage.queue.ConnectionQueue;
import io.aquaticlabs.aquaticdata.data.storage.queue.ConnectionRequest;
import lombok.Getter;

import java.util.List;

/**
 * @Author: extremesnow
 * On: 8/21/2022
 * At: 22:06
 */
public abstract class ADatabase {

    @Getter
    private final String table;

    @Getter
    private final ConnectionQueue connectionQueue;

    protected ADatabase(String table) {
        this.table = table;
        connectionQueue = new ConnectionQueue(this);
    }

    public abstract <T> T executeNonLockConnection(ConnectionRequest<T> connectionRequest);

    public abstract void shutdown();

    public abstract String insertStatement(List<DataEntry<String, String>> columns);

    public abstract String buildUpdateStatementSQL(List<DataEntry<String, String>> columns);

    public abstract void createTable(List<DataEntry<String, ColumnType>> columns, boolean force);

    public abstract void verifyTableExists(List<DataEntry<String, ColumnType>> columns);

    public abstract void dropTable();

}
