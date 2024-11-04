package io.aquaticlabs.aquaticdata.type.sql;

import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.cache.ModelCachedData;
import io.aquaticlabs.aquaticdata.model.SerializedData;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.model.StorageValue;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.storage.Storage;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import io.aquaticlabs.aquaticdata.util.StorageUtil;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 11/3/2024
 * At: 15:30
 */
@Getter
public abstract class SQLDatabase<T extends StorageModel> extends HikariCPDatabase<T> {

    private final SQLCredential credential;
    @Setter
    private int batchSize = 250; // Adjust the batch size as needed

    protected SQLDatabase(SQLCredential credential, DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        super(tableStructure, serializer, asyncExecutor, syncExecutor);
        this.credential = credential;
    }


    public <K> void start(Storage<K, T> holder) {
        start(holder, true);
    }

    public <K> void start(Storage<K, T> holder, boolean async) {
        confirmTable(getTableStructure()).whenComplete((b, t) -> loadAll(holder, async));
        // load a cache ?
    }

    protected boolean doesEntryExist(Connection connection, DataEntry<String, ?> key) {
        String sql = "SELECT 1 FROM " + credential.getTableName() + " WHERE " + key.getKey() + " = '" + key.getValue().toString() + "'";
        ResultSet resultSet = null;
        try {
            resultSet = connection.createStatement().executeQuery(sql);
            return resultSet.next();
        } catch (SQLException e) {
            throw new IllegalStateException("Error while checking if entry exists in database", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                DataDebugLog.logError("SQLException: " + e.getMessage());
            }
        }
    }

    @Override
    public CompletableFuture<Boolean> confirmTable(DatabaseStructure tableStructure) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>(connection -> connection.createStatement().executeUpdate(createTableStatement(false)), getSyncExecutor()));
        executeRequest(new ConnectionRequest<>(connection -> {

            Set<String> removeColumns = new HashSet<>();
            Map<String, SQLColumnType> retypeColumns = new LinkedHashMap<>();
            Map<String, String> moveColumns = new LinkedHashMap<>();
            Map<String, Map.Entry<String, SQLColumnType>> addColumns = new LinkedHashMap<>();

            // is table good?
            if (!verifyColumns(connection, removeColumns, retypeColumns, moveColumns, addColumns)) {
                future.complete(true);
                return true;
            }

            correctColumns(connection, removeColumns, retypeColumns, moveColumns, addColumns);
            future.complete(false);
            return false;
        }, getSyncExecutor()));
        return future;
    }

/*
    @Override
    public <S extends Iterable<T>> CompletableFuture<List<T>> saveLoaded(S loaded, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();

        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(connection -> {
            int modified = 0;
            List<T> saved = new ArrayList<>();
            try (Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                try {
                    for (T object : loaded) {

                        DataDebugLog.logDebug("Saving Loaded:" + object.getKey());

                        SerializedData data = new SerializedData();
                        getSerializer().serialize(object, data);


                        DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);

                        // If the size is 1, it should only contain the key.
                        if (needsUpdate.getColumnValues().size() == 1) {
                            DataDebugLog.logDebug("Needs update contains no data values. no need for updating");
                            continue;
                        }

                        modified++;
                        DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());
                        if (doesEntryExist(connection, modifiedStructure.getFirstValuePair())) {

                            try {
                                DataDebugLog.logDebug("Adding Update Batch Statement");
                                statement.addBatch(updateStatement(needsUpdate));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug("Failed adding batch Data: " + e.getMessage());
                            }
                        } else {
                            try {
                                DataDebugLog.logDebug("Adding Insert Batch Statement");
                                statement.addBatch(insertStatement(modifiedStructure));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug("Fail Inserting Data: " + e.getMessage());
                            }
                        }


                        if (modified % batchSize == 0) {
                            try {
                                statement.executeBatch();
                                statement.clearBatch();

                                DataDebugLog.logDebug("Success executing batch of " + modified);

                            } catch (SQLException e) {
                                DataDebugLog.logDebug("Failed executing batch: " + e.getMessage());
                            }
                        }
                    }
                    DataDebugLog.logDebug("Executing Last batch of " + modified);
                    statement.executeBatch();

                    connection.commit(); // Commit the transaction
                } catch (SQLException e) {
                    connection.rollback();
                }
                connection.setAutoCommit(true);

                future.complete(saved);
                DataDebugLog.logDebug("Saved Loaded, Modified " + modified + " users.");
                return true;
            }
        }, executor));
        return future;
    }*/

    @Override
    public <S extends Iterable<T>> CompletableFuture<List<T>> saveLoaded(S loaded, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();

        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(connection -> {
            List<T> saved = new ArrayList<>();

            int modified = 0;

            try (PreparedStatement updateStmt = connection.prepareStatement(updatePreparedStatement(getTableStructure()));
                 PreparedStatement insertStmt = connection.prepareStatement(insertPreparedStatement(getTableStructure()))) {

                connection.setAutoCommit(false);
                int batchCount = 0;

                Map<Object, Boolean> existingEntries = cacheExistingEntries(connection, loaded);

                for (T object : loaded) {
                    DataDebugLog.logDebug("Saving Loaded: " + object.getKey());

                    SerializedData data = new SerializedData();
                    getSerializer().serialize(object, data);

                    DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);

                    // If the size is 1, it should only contain the key.
                    if (needsUpdate.getColumnValues().size() == 1) {
                        DataDebugLog.logDebug("Needs update contains no data values. no need for updating");
                        continue;
                    }
                    modified++;
                    DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());

                    boolean exists = existingEntries.containsKey(modifiedStructure.getFirstValuePair().getKey());
                    PreparedStatement batchStmt = exists ? updateStmt : insertStmt;
                    addBatch(batchStmt, modifiedStructure);

                    saved.add(object);
                    batchCount++;

                    // Execute batch every 'batchSize' entries
                    if (batchCount % batchSize == 0) {
                        executeAndClearBatch(updateStmt, insertStmt);
                        DataDebugLog.logDebug("Executed batch of " + batchSize);
                    }
                }

                // Execute the final batch
                executeAndClearBatch(updateStmt, insertStmt);
                connection.commit();
                DataDebugLog.logDebug("Executed final batch and committed.");
                DataDebugLog.logDebug("Saved Loaded, Modified " + modified + " users.");
                future.complete(saved);
            } catch (SQLException e) {
                try {
                    connection.rollback();
                    DataDebugLog.logDebug("Transaction rolled back due to error: " + e.getMessage());
                } catch (SQLException rollbackEx) {
                    DataDebugLog.logDebug("Rollback failed: " + rollbackEx.getMessage());
                }
            } finally {
                try {
                    connection.setAutoCommit(true);
                } catch (SQLException e) {
                    DataDebugLog.logDebug("Failed to reset auto-commit: " + e.getMessage());
                }
            }
            return true;
        }, executor));
        return future;
    }

    @Override
    public CompletableFuture<List<T>> saveList(List<T> list, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();

        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(connection -> {
            int modified = 0;
            List<T> saved = new ArrayList<>();
            try (Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                try {
                    for (T object : list) {

                        DataDebugLog.logDebug("Saving List: " + object.getKey());

                        SerializedData data = new SerializedData();
                        getSerializer().serialize(object, data);

                        DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);

                        // If the size is 1, it should only contain the key.
                        if (needsUpdate.getColumnValues().size() == 1) {
                            DataDebugLog.logDebug("Needs update contains no data values. no need for updating");
                            continue;
                        }

                        modified++;
                        DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());
                        if (doesEntryExist(connection, modifiedStructure.getFirstValuePair())) {

                            try {
                                DataDebugLog.logDebug("Adding Update Batch Statement");
                                statement.addBatch(updateStatement(needsUpdate));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug("Failed adding batch Data: " + e.getMessage());
                            }
                        } else {
                            try {
                                DataDebugLog.logDebug("Adding Insert Batch Statement");
                                statement.addBatch(insertStatement(modifiedStructure));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug("Fail Inserting Data: " + e.getMessage());
                            }
                        }


                        if (modified % batchSize == 0) {
                            try {
                                statement.executeBatch();
                                statement.clearBatch();

                                DataDebugLog.logDebug("Success executing batch of " + modified);

                            } catch (SQLException e) {
                                DataDebugLog.logDebug("Failed executing batch: " + e.getMessage());
                            }
                        }
                    }
                    DataDebugLog.logDebug("Executing Last batch of " + modified);
                    statement.executeBatch();

                    connection.commit(); // Commit the transaction
                } catch (SQLException e) {
                    connection.rollback();
                }
                connection.setAutoCommit(true);

                future.complete(saved);
                DataDebugLog.logDebug("Saved List, Modified " + modified + " users.");
                return true;
            }
        }, executor));
        return future;
    }

    @Override
    public CompletableFuture<T> save(T object, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<T> future = new CompletableFuture<>();

        SerializedData data = new SerializedData();
        getSerializer().serialize(object, data);

        DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);
        // If the size is 1, it should only contain the key.
        if (needsUpdate.getColumnValues().size() == 1) {
            DataDebugLog.logDebug("Needs update contains no data values. no need for updating");
            return null;
        }

        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(connection -> {
            DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());

            if (doesEntryExist(connection, modifiedStructure.getFirstValuePair())) {
                try {
                    connection.createStatement().executeUpdate(updateStatement(needsUpdate));
                    DataDebugLog.logDebug("Success Updating Data");
                } catch (SQLException e) {
                    DataDebugLog.logDebug("Fail Updating Data: " + e.getMessage());
                }
            } else {
                try {
                    connection.createStatement().executeUpdate(insertStatement(modifiedStructure));
                    DataDebugLog.logDebug("Success Inserting Data");
                } catch (SQLException e) {
                    DataDebugLog.logDebug("Fail Inserting Data: " + e.getMessage());
                }
            }
            future.complete(object);
            return true;
        }, executor));

        return future;
    }

    @Override
    public <K> CompletableFuture<T> load(Storage<K, T> holder, DataEntry<String, K> key, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<T> future = new CompletableFuture<>();

        executeRequest(new ConnectionRequest<>(connection -> {
            if (!doesEntryExist(connection, key)) {
                // Future never gets completed if the entry doesn't exist.
                future.complete(null);
                return false;
            }

            final String sql = "SELECT * FROM " + credential.getTableName() + " WHERE " + key.getKey() + " = '" + key.getValue() + "'";

            try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
                int column = 1;
                List<StorageValue> data = new LinkedList<>();
                for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
                    data.add(new StorageValue(entry.getKey(), rs.getObject(column), SQLColumnType.matchType(rs.getMetaData().getColumnTypeName(column))));
                    column++;
                }

                DataDebugLog.logDebug(sql);
                SerializedData serializedData = new SerializedData();
                serializedData.fromQuery(data);
                T dummy = getSerializer().deserialize(holder.get(key.getValue()), serializedData);
                loadIntoCache(dummy, serializedData);
                holder.add(dummy);
                future.complete(dummy);
            } catch (SQLException e) {
                DataDebugLog.logError("Failed To Load user: ", e);
            }
            return true;
        }, executor));
        return future;
    }

/*    @Override
    public <K> CompletableFuture<List<T>> loadAll(Storage<K, T> holder, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>(); // creates a future here

        executeRequest(new ConnectionRequest<>(conn -> {
            List<T> loaded = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + credential.getTableName())) {
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    List<StorageValue> data = new LinkedList<>();
                    int column = 1;
                    for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
                        //todo dont call rs.getMetaData
                        SQLColumnType columnType = SQLColumnType.matchType(rs.getMetaData().getColumnTypeName(column));


                        data.add(new StorageValue(entry.getKey(), rs.getObject(entry.getKey()), columnType));
                        column++;
                    }
                    SerializedData serializedData = new SerializedData();
                    serializedData.fromQuery(data);

                    try {
                        T dummy = getSerializer().deserialize(holder.get(serializedData.applyAs(data.get(0).getField(), holder.getKeyClass(), null)), serializedData);
                        loadIntoCache(dummy, serializedData);
                        holder.add(dummy);
                        loaded.add(dummy);
                    } catch (Exception exception) {
                        DataDebugLog.logDebug("Failed to deserialize class, with data: " + serializedData);
                        DataDebugLog.logError(exception.getMessage());
                    }
                }
            } catch (SQLException e) {
                DataDebugLog.logError("Failed To Load All users" + e.getMessage());
            }

            future.complete(loaded);
            return null;
        }, executor));
        return future;
    }*/

    @Override
    public <K> CompletableFuture<List<T>> loadAll(Storage<K, T> holder, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>(conn -> {
            List<T> loaded = new ArrayList<>();
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                stmt.setFetchSize(Integer.MIN_VALUE); // Stream results one by one for MySQL/MariaDB
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + credential.getTableName());

                // Prepare static column type mappings to avoid repeated calls to ResultSet metadata
                List<String> columnNames = new ArrayList<>();
                List<SQLColumnType> columnTypes = new ArrayList<>();
                for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
                    columnNames.add(entry.getKey());
                    columnTypes.add(SQLColumnType.matchType(entry.getValue().getSql()));
                }

                // Collect ResultSet rows in a batch list for parallel processing
                List<List<StorageValue>> rowData = new ArrayList<>();
                while (rs.next()) {
                    List<StorageValue> data = new ArrayList<>();
                    for (int i = 0; i < columnNames.size(); i++) {
                        data.add(new StorageValue(columnNames.get(i), rs.getObject(columnNames.get(i)), columnTypes.get(i)));
                    }
                    rowData.add(data);
                }

                // Parallelize deserialization and loading into cache
                rowData.parallelStream().forEach(data -> {
                    SerializedData serializedData = new SerializedData();
                    serializedData.fromQuery(data);
                    try {
                        T dummy = getSerializer().deserialize(holder.get(serializedData.applyAs(data.get(0).getField(), holder.getKeyClass(), null)), serializedData);
                        loadIntoCache(dummy, serializedData);
                        holder.add(dummy);
                        loaded.add(dummy);
       /*                 synchronized (loaded) { // Synchronize only if necessary

                        }*/
                    } catch (Exception exception) {
                        DataDebugLog.logDebug("Failed to deserialize class, with data: " + serializedData);
                        DataDebugLog.logError(exception.getMessage());
                    }
                });
            } catch (SQLException e) {
                DataDebugLog.logError("Failed to load all users: " + e.getMessage());
            }
            future.complete(loaded);
            return null;
        }, executor));
        return future;
    }

    @Override
    public CompletableFuture<List<T>> getKeyedList(String keyColumn, String keyValue, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>(conn -> {
            List<T> loaded = new ArrayList<>();
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + credential.getTableName() + " WHERE " + keyColumn + " = '" + keyValue + "' ;");

                // Prepare static column type mappings to avoid repeated calls to ResultSet metadata
                List<String> columnNames = new ArrayList<>();
                List<SQLColumnType> columnTypes = new ArrayList<>();
                for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
                    columnNames.add(entry.getKey());
                    columnTypes.add(SQLColumnType.matchType(entry.getValue().getSql()));
                }

                // Collect ResultSet rows in a batch list for parallel processing
                List<List<StorageValue>> rowData = new ArrayList<>();
                while (rs.next()) {
                    List<StorageValue> data = new ArrayList<>();
                    for (int i = 0; i < columnNames.size(); i++) {
                        data.add(new StorageValue(columnNames.get(i), rs.getObject(columnNames.get(i)), columnTypes.get(i)));
                    }
                    rowData.add(data);
                }

                // Parallelize deserialization and loading into cache
                rowData.parallelStream().forEach(data -> {
                    SerializedData serializedData = new SerializedData();
                    serializedData.fromQuery(data);
                    try {
                        T entry = getSerializer().deserialize(construct(getVariant(credential.getTableName())), serializedData);
                        loadIntoCache(entry, serializedData);
                        loaded.add(entry);
                    } catch (Exception exception) {
                        DataDebugLog.logDebug("Failed to deserialize class, with data: " + serializedData);
                        DataDebugLog.logError(exception.getMessage());
                    }
                });
            } catch (SQLException e) {
                DataDebugLog.logError("Failed to get KEY users: " + e.getMessage());
            }
            future.complete(loaded);
            return null;
        }, executor));
        return future;
    }

    private DatabaseStructure buildNeedsUpdate(T object, SerializedData data) {
        ModelCachedData cachedData = getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());

        // Check the cache to see if there's outdated data.
        DatabaseStructure needsUpdate = new DatabaseStructure(credential.getTableName());
        boolean first = true;
        for (Map.Entry<String, Object> entry : data.toDatabaseStructure(getTableStructure()).getColumnValues().entrySet()) {
            String column = entry.getKey();
            String value = entry.getValue() == null ? getTableStructure().getColumnDefaults().get(column).toString() : entry.getValue().toString();
            if (first) {
                needsUpdate.addValue(column, getTableStructure().getColumnStructure().get(column), value);
                first = false;
                continue;
            }
            if (!data.getValue(entry.getKey()).isPresent() || cachedData.isOutdated(column, value)) {
                needsUpdate.addValue(column, getTableStructure().getColumnStructure().get(column), value);
                DataDebugLog.logDebug("Needs Update: " + column + " " + value);
            }
        }
        return needsUpdate;
    }

    protected boolean verifyColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnType>> addColumns) {
        boolean needsAltering = false;
        try {

            List<String> structureColumns = new ArrayList<>(getTableStructure().getColumnStructure().keySet());

            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet result = metaData.getColumns(null, null, credential.getTableName(), null);

            Map<String, DataEntry<String, String>> databaseColumns = new LinkedHashMap<>();
            while (result.next()) {
                String colName = result.getString("COLUMN_NAME");
                databaseColumns.put(colName, new DataEntry<>(colName, result.getString("TYPE_NAME")));
            }

            // This column name isn't inside the structure
            for (String col : databaseColumns.keySet()) {
                if (!structureColumns.contains(col)) {
                    removeColumns.add(col);
                    needsAltering = true;
                }
            }


            int addCurrent = 0;
            for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
                if (!databaseColumns.containsKey(entry.getKey())) {
                    addColumns.put(structureColumns.get(addCurrent - 1), entry);
                    needsAltering = true;
                }
                addCurrent++;
            }


            Map<String, DataEntry<String, String>> dataClone = new LinkedHashMap<>(databaseColumns);

            for (String col : removeColumns) {
                dataClone.remove(col);
            }

            List<String> dataMirrorArray = getDataMirrorArray(addColumns, dataClone, structureColumns);

            StorageUtil.calculateMoves(moveColumns, dataMirrorArray, structureColumns);
            if (!moveColumns.isEmpty()) {
                needsAltering = true;
            }


            for (DataEntry<String, String> dataEntry : databaseColumns.values()) {
                if (!getTableStructure().getColumnStructure().containsKey(dataEntry.getKey())) {
                    continue;
                }
                SQLColumnType structureColumnType = getTableStructure().getColumnStructure().get(dataEntry.getKey());

                String databaseColumnName = dataEntry.getKey();
                String databaseColumnTypeName = dataEntry.getValue();
                SQLColumnType databaseColType = SQLColumnType.matchType(databaseColumnTypeName);

                // Column type is wrong
                if (databaseColType == null || !SQLColumnType.isSimilarMatching(structureColumnType, databaseColType)) {
                    needsAltering = true;
                    retypeColumns.put(databaseColumnName, structureColumnType);
                }
            }


        } catch (Exception e) {
            throw new IllegalStateException("Failed to confirm table, strange Column Types/Names.", e);
        }
        DataDebugLog.logDebug("Table Needs Alter: " + needsAltering);

        return needsAltering;
    }

    private static List<String> getDataMirrorArray(Map<String, Map.Entry<String, SQLColumnType>> addColumns, Map<String, DataEntry<String, String>> dataClone, List<String> structureColumns) {
        List<String> dataMirrorArray = new ArrayList<>(dataClone.keySet());

        for (Map.Entry<String, Map.Entry<String, SQLColumnType>> colEntry : addColumns.entrySet()) {
            String colName = colEntry.getValue().getKey();
            int addAtInt = 0;
            for (String col : structureColumns) {
                if (colName.equals(col)) {
                    break;
                }
                addAtInt++;
            }
            if (dataMirrorArray.size() < addAtInt) {
                dataMirrorArray.add(colName);
                continue;
            }
            dataMirrorArray.add(addAtInt, colName);
        }
        return dataMirrorArray;
    }

    // Helper method to add batch to statement
    private void addBatch(PreparedStatement stmt, DatabaseStructure structure) throws SQLException {
        // Fill in statement parameters based on DatabaseStructure

        int i = 1;
        for (Map.Entry<String, Object> entry : structure.getColumnValues().entrySet()) {
            stmt.setObject(i, entry.getValue());
        }

        // Example: stmt.setObject(1, structure.getColumnValue(...));
        stmt.addBatch();
    }

    // Execute batch and clear
    private void executeAndClearBatch(PreparedStatement... statements) throws SQLException {
        for (PreparedStatement stmt : statements) {
            stmt.executeBatch();
            stmt.clearBatch();
        }
    }

    // Cache existing entries for fast lookup
    private Map<Object, Boolean> cacheExistingEntries(Connection connection, Iterable<T> loaded) throws SQLException {
        Map<Object, Boolean> existingEntries = new HashMap<>();
        String keyColumn = getTableStructure().getFirstValuePair().getKey();
        try (PreparedStatement checkStmt = connection.prepareStatement("SELECT " + keyColumn + " FROM " + credential.getTableName())) {
            ResultSet rs = checkStmt.executeQuery();
            while (rs.next()) {
                existingEntries.put(rs.getObject(keyColumn), true);
            }
        }
        return existingEntries;
    }

    public <S> void executeSQLRequest(ConnectionRequest<S> request) {
        executeRequest(request);
    }


    public abstract String createTableStatement(boolean force);

    protected abstract void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnType>> addColumns);

    public abstract String insertStatement(DatabaseStructure modifiedStructure);

    public abstract String insertPreparedStatement(DatabaseStructure modifiedStructure);

    public abstract String updateStatement(DatabaseStructure modifiedStructure);

    public abstract String updatePreparedStatement(DatabaseStructure modifiedStructure);

    public abstract void dropTable();

}
