package io.aquaticlabs.aquaticdata.type.sql;

import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.cache.ModelCachedData;
import io.aquaticlabs.aquaticdata.model.SerializedData;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.model.StorageValue;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.storage.Storage;
import io.aquaticlabs.aquaticdata.type.ColumnData;
import io.aquaticlabs.aquaticdata.type.sql.sqlite.SQLiteCredential;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.DataDebugLogType;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * @Author: extremesnow
 * On: 11/3/2024
 * At: 15:30
 */
@Getter
public abstract class SQLDatabase<T extends StorageModel> extends HikariCPDatabase<T> {

    private final SQLCredential credential;
    @Setter
    private int batchSize = 500; // Adjust the batch size as needed

    private boolean sqliteCredential = false;

    protected SQLDatabase(SQLCredential credential, DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        super(tableStructure, serializer, asyncExecutor, syncExecutor);
        this.credential = credential;
        tableStructure.setTableName(credential.getTableName());
        if (credential instanceof SQLiteCredential) {
            sqliteCredential = true;
        }

    }


    public <K> void start(Storage<K, T> holder) {
        start(holder, true);
    }

    public <K> void start(Storage<K, T> holder, boolean async) {
        confirmTable(getTableStructure());
        // load a cache ? // todo?
    }

    protected boolean doesEntryExist(Connection connection, DataEntry<String, ?> key) {
        String sql = "SELECT 1 FROM " + credential.getTableName() + " WHERE " + key.getKey() + " = '" + key.getValue().toString() + "'";
        DataDebugLog.logDebug(DataDebugLogType.SQL_QUERIES, sql);

        ResultSet resultSet = null;
        try {
            resultSet = connection.createStatement().executeQuery(sql);
            boolean exists = resultSet.next();
            DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, "Entry: " + key.getValue().toString() + " Exist: " + exists);

            return exists;
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
            Map<String, Map.Entry<String, SQLColumnData<?>>> addColumns = new LinkedHashMap<>();

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

    @Override
    public <S extends Iterable<T>> CompletableFuture<List<T>> saveLoaded(S loaded, boolean async) {
        return saveLoaded(loaded, async, true);
    }

    /**
     * Saves or updates a collection of loaded objects in the database using batch processing.
     * If an object already exists (based on its key), it is updated; otherwise, a new entry is inserted.
     * The method optimizes database calls by caching existing entries and executing batch updates.
     * Transactions are committed after processing for efficiency.
     *
     * @param loaded    The collection of objects to be saved or updated.
     * @param async     Whether to execute the operation asynchronously.
     * @param useRunner Whether to use a dedicated executor for the operation.
     * @param <S>       The iterable type that extends {@link Iterable} containing objects of type {@code T}.
     * @return A {@link CompletableFuture} containing the list of successfully saved objects.
     */
    @Override
    public <S extends Iterable<T>> CompletableFuture<List<T>> saveLoaded(S loaded, boolean async, boolean useRunner) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();

        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(connection -> {
            int modified = 0;
            List<T> saved = new ArrayList<>();
            try (Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);

                Map<Object, Boolean> existingEntries = cacheExistingEntries(connection);

                try {
                    for (T object : loaded) {

                        DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Saving Loaded:" + object.getKey());

                        SerializedData data = new SerializedData();
                        getSerializer().serialize(object, data);


                        DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);

                        // If the size is 1, it should only contain the key.
                        if (needsUpdate.getColumnStructure().size() == 1) {
                            DataDebugLog.logDebug(DataDebugLogType.SQL_UPDATE, getDataClass().getSimpleName() + " Database: Needs update contains no data values. no need for updating");
                            continue;
                        }


                        DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());
                        boolean exists = existingEntries.containsKey(object.getKey());
/*
                        System.out.println(object.getKey());
                        System.out.println(existingEntries.size());
                        System.out.println(existingEntries.keySet());
*/

                        DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: exists: " + exists);

                        if (exists) {
                            try {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_UPDATE, getDataClass().getSimpleName() + " Database: Adding Update Batch Statement");
                                statement.addBatch(updateStatement(needsUpdate));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Failed adding batch Data: " + e.getMessage());
                                continue;
                            }
                        } else {
                            try {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_INSERT, getDataClass().getSimpleName() + " Database: Adding Insert Batch Statement");
                                statement.addBatch(insertStatement(modifiedStructure));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Fail Inserting Data: " + e.getMessage());
                                continue;
                            }
                        }
                        modified++;

                        if (modified % batchSize == 0) {
                            try {
                                statement.executeBatch();
                                statement.clearBatch();
                                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Success executing batch of " + modified);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Failed executing batch: " + e.getMessage());
                            }
                        }
                    }
                    DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Executing Last batch of " + modified);
                    statement.executeBatch();
                    statement.clearBatch();
                    connection.commit(); // Commit the transaction
                } catch (SQLException e) {
                    connection.rollback();
                }
                connection.setAutoCommit(true);

                future.complete(saved);
                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Saved Loaded, Modified " + modified + " users.");
                return true;
            }
        }, useRunner ? executor : null));
        return future;
    }

    /**
     * Saves or updates a list of objects in the database using batch processing.
     * If an object already exists (based on its key), it is updated; otherwise, a new entry is inserted.
     * Batch execution is used for efficiency, and transactions are committed after processing.
     *
     * @param list  The list of objects to be saved or updated.
     * @param async Whether to execute the operation asynchronously.
     * @return A {@link CompletableFuture} containing the list of successfully saved objects.
     */
    @Override
    public CompletableFuture<List<T>> saveList(List<T> list, boolean async) {
        return saveList(list, null, async);
    }

    /**
     * Saves or updates a list of objects in the database using batch processing.
     * If an object already exists (based on its key), it is updated; otherwise, a new entry is inserted.
     * Batch execution is used for efficiency, and transactions are committed after processing.
     *
     * @param list            The list of objects to be saved or updated.
     * @param updateStructure The structure of which should be updated.
     * @param async           Whether to execute the operation asynchronously.
     * @return A {@link CompletableFuture} containing the list of successfully saved objects.
     */
    @Override
    public CompletableFuture<List<T>> saveList(List<T> list, DatabaseStructure updateStructure, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();

        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(connection -> {
            int modified = 0;
            List<T> saved = new ArrayList<>();
            try (Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                try {
                    for (T object : list) {

                        DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Saving List: " + object.getKey());

                        SerializedData data = new SerializedData();
                        getSerializer().serialize(object, data);


                        DatabaseStructure needsUpdate = buildNeedsUpdateFromStruct(object, updateStructure, data);

                        // If the size is 1, it should only contain the key.
                        if (needsUpdate.getColumnStructure().size() == 1) {
                            DataDebugLog.logDebug(DataDebugLogType.SQL_UPDATE, getDataClass().getSimpleName() + " Database: Needs update contains no data values. no need for updating");
                            continue;
                        }

                        modified++;
                        DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());
                        DataEntry<String, String> keyPair = modifiedStructure.getKeyValuePair();
                        DataDebugLog.logDebug(DataDebugLogType.SQL_QUERIES, "Checking existence for key=" + keyPair.getKey() + ", value=" + keyPair.getValue());

                        if (doesEntryExist(connection, keyPair)) {
                            try {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_UPDATE, getDataClass().getSimpleName() + " Database: Adding Update Batch Statement");
                                statement.addBatch(updateStatement(needsUpdate));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Failed adding batch Data: " + e.getMessage());
                            }
                        } else {
                            try {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_INSERT, getDataClass().getSimpleName() + " Database: Adding Insert Batch Statement");
                                statement.addBatch(insertStatement(modifiedStructure));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_INSERT, getDataClass().getSimpleName() + " Database: Fail Inserting Data: " + e.getMessage());
                            }
                        }


                        if (modified % batchSize == 0) {
                            try {
                                statement.executeBatch();
                                statement.clearBatch();

                                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Success executing batch of " + modified);

                            } catch (SQLException e) {
                                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Failed executing batch: " + e.getMessage());
                            }
                        }
                    }
                    DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Executing Last batch of " + modified);
                    statement.executeBatch();

                    connection.commit(); // Commit the transaction
                } catch (SQLException e) {
                    connection.rollback();
                }
                connection.setAutoCommit(true);

                future.complete(saved);
                DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Saved List, Modified " + modified + " users.");
                return true;
            }
        }, executor));
        return future;
    }

    /**
     * Saves or updates the given object in the database.
     * If the object already exists in the database (based on its key), the method updates it.
     * Otherwise, it inserts a new entry.
     *
     * @param object The object of type {@code T} to be saved or updated.
     * @param async  Whether to execute the operation asynchronously.
     * @return A {@link CompletableFuture} containing the saved object.
     */
    @Override
    public CompletableFuture<T> save(T object, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<T> future = new CompletableFuture<>();

        SerializedData data = new SerializedData();
        getSerializer().serialize(object, data);

        DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);
        // If the size is 1, it should only contain the key.
        if (needsUpdate.getColumnStructure().size() == 1) {
            DataDebugLog.logDebug(DataDebugLogType.SQL_SAVING, getDataClass().getSimpleName() + " Database: Needs update contains no data values. no need for updating");
            return null;
        }

        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(connection -> {
            DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());

            if (doesEntryExist(connection, modifiedStructure.getKeyValuePair())) {
                try {
                    connection.createStatement().executeUpdate(updateStatement(needsUpdate));
                    DataDebugLog.logDebug(DataDebugLogType.SQL_UPDATE, getDataClass().getSimpleName() + " Database: Success Updating Data");
                } catch (SQLException e) {
                    DataDebugLog.logDebug(DataDebugLogType.SQL_UPDATE, getDataClass().getSimpleName() + " Database: Fail Updating Data: " + e.getMessage());
                }
            } else {
                try {
                    connection.createStatement().executeUpdate(insertStatement(modifiedStructure));
                    DataDebugLog.logDebug(DataDebugLogType.SQL_INSERT, getDataClass().getSimpleName() + " Database: Success Inserting Data");
                } catch (SQLException e) {
                    DataDebugLog.logDebug(DataDebugLogType.SQL_INSERT, getDataClass().getSimpleName() + " Database: Fail Inserting Data: " + e.getMessage());
                }
            }
            future.complete(object);
            return true;
        }, executor));

        return future;
    }


    @Override
    public <K> CompletableFuture<T> load(Storage<K, T> holder, DataEntry<String, K> key, boolean async) {
        return load(holder, key, async, false);
    }

    /**
     * Loads a single record from the database based on the provided key.
     * If the entry exists, it retrieves the corresponding row, deserializes the data,
     * caches the result, and adds it to the provided storage holder.
     *
     * @param holder  The {@link Storage} instance where the loaded record will be stored.
     * @param key     The {@link DataEntry} containing the column name and key value to query.
     * @param async   Whether to execute the query asynchronously.
     * @param persist Whether to persist the loaded entry in storage.
     * @param <K>     The key type of the storage holder.
     * @return A {@link CompletableFuture} containing the deserialized object of type {@code T}, or {@code null} if not found.
     */
    @Override
    public <K> CompletableFuture<T> load(Storage<K, T> holder, DataEntry<String, K> key, boolean async, boolean persist) {
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
                for (Map.Entry<String, ColumnData<?>> entry : getTableStructure().getColumnStructure().entrySet()) {
                    data.add(new StorageValue(entry.getKey(), rs.getObject(column), SQLColumnType.matchType(rs.getMetaData().getColumnTypeName(column))));
                    column++;
                }

                DataDebugLog.logDebug(DataDebugLogType.SQL_QUERIES, sql);
                SerializedData serializedData = new SerializedData();
                serializedData.fromQuery(data);
                T dummy = getSerializer().deserialize(holder.get(key.getValue()), serializedData);
                loadIntoCache(dummy, serializedData);
                holder.add(dummy, persist);
                future.complete(dummy);
            } catch (SQLException e) {
                DataDebugLog.logError("Failed To Load user: ", e);
            }
            return true;
        }, executor));
        return future;
    }

    /**
     * Loads all records from the database into the specified storage holder.
     * This method retrieves all rows from the table, deserializes the data, and caches the results.
     * It uses batch processing and parallel streams for efficient handling of large datasets.
     *
     * @param holder The {@link Storage} instance where the loaded records will be stored.
     * @param async  Whether to execute the query asynchronously.
     * @param <K>    The key type of the storage holder.
     * @return A {@link CompletableFuture} containing a list of deserialized objects of type {@code T}.
     */
    @Override
    public <K> CompletableFuture<List<T>> loadAll(Storage<K, T> holder, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>(conn -> {
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                if (!sqliteCredential) {
                    stmt.setFetchSize(Integer.MIN_VALUE); // Stream results one by one for MySQL/MariaDB
                }
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + credential.getTableName());

                // Prepare static column type mappings to avoid repeated calls to ResultSet metadata
                List<String> columnNames = new ArrayList<>();
                List<SQLColumnType> columnTypes = new ArrayList<>();
                for (Map.Entry<String, ColumnData<?>> entry : getTableStructure().getColumnStructure().entrySet()) {
                    SQLColumnData<?> columnData = (SQLColumnData<?>) entry.getValue();
                    columnNames.add(entry.getKey());
                    columnTypes.add(columnData.getColumnType());
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
                List<T> loaded = rowData.parallelStream().map(data -> {
                    SerializedData serializedData = new SerializedData();
                    serializedData.fromQuery(data);
                    try {
                        T dummy = getSerializer().deserialize(holder.get(serializedData.applyAs(data.get(0).getField(), holder.getKeyClass(), null)), serializedData);
                        loadIntoCache(dummy, serializedData);
                        holder.add(dummy);
                        return dummy;

                    } catch (Exception exception) {
                        DataDebugLog.logDebug(DataDebugLogType.SQL_LOADING, getDataClass().getSimpleName() + " Database: Failed to deserialize class, with data: " + serializedData);
                        DataDebugLog.logError(exception.getMessage());
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toList());

                future.complete(loaded);
            } catch (SQLException e) {
                DataDebugLog.logError("Failed to load all users: " + e.getMessage());
            }

            return null;
        }, executor));
        return future;
    }


    /**
     * Retrieves a list of objects from the database based on a specific key column and value.
     * This method queries the database for rows where the specified key column matches the given key value,
     * deserializes the results, and loads them into a list.
     *
     * @param keyColumn The column name to filter the query results.
     * @param keyValue  The value to match in the specified key column.
     * @param async     Whether to execute the query asynchronously.
     * @return A {@link CompletableFuture} containing a list of deserialized objects of type {@code T}.
     */
    @Override
    public CompletableFuture<List<T>> getKeyedList(String keyColumn, String keyValue, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>(conn -> {
            List<T> loaded = new ArrayList<>();
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + credential.getTableName() + " WHERE " + keyColumn + " = '" + keyValue + "' ;");

                List<String> columnNames = new ArrayList<>();
                List<SQLColumnType> columnTypes = new ArrayList<>();
                for (Map.Entry<String, ColumnData<?>> entry : getTableStructure().getColumnStructure().entrySet()) {
                    SQLColumnData<?> columnData = (SQLColumnData<?>) entry.getValue();
                    columnNames.add(entry.getKey());
                    columnTypes.add(columnData.getColumnType());
                }

                List<List<StorageValue>> rowData = new ArrayList<>();
                while (rs.next()) {
                    List<StorageValue> data = new ArrayList<>();
                    for (int i = 0; i < columnNames.size(); i++) {
                        data.add(new StorageValue(columnNames.get(i), rs.getObject(columnNames.get(i)), columnTypes.get(i)));
                    }
                    rowData.add(data);
                }

                rowData.parallelStream().forEach(data -> {
                    SerializedData serializedData = new SerializedData();
                    serializedData.fromQuery(data);
                    try {
                        T entry = getSerializer().deserialize(construct(getDataClass()), serializedData);
                        loadIntoCache(entry, serializedData);
                        loaded.add(entry);
                    } catch (Exception exception) {
                        DataDebugLog.logDebug(DataDebugLogType.SQL_LOADING, getDataClass().getSimpleName() + " Database: Failed to deserialize class, with data: " + serializedData);
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


    @Override
    public <K> CompletableFuture<Map<K, SimpleStorageModel>> getStorageModelMap(List<String> keyColumns, boolean async, Class<K> keyClass) {
        Executor executor = getExecutor(async);
        CompletableFuture<Map<K, SimpleStorageModel>> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>(conn -> {
            Map<K, SimpleStorageModel> modelMap = new ConcurrentHashMap<>();
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                String query = "SELECT " + String.join(", ", keyColumns) + " FROM " + credential.getTableName() + ";";
                DataDebugLog.logDebug(DataDebugLogType.SQL_QUERIES, query);
                ResultSet rs = stmt.executeQuery(query);

                List<SQLColumnType> columnTypes = new ArrayList<>();
                for (Map.Entry<String, ColumnData<?>> entry : getTableStructure().getColumnStructure().entrySet()) {
                    SQLColumnData<?> columnData = (SQLColumnData<?>) entry.getValue();
                    if (keyColumns.contains(entry.getKey())) {
                        columnTypes.add(columnData.getColumnType());
                    }
                }

                List<List<StorageValue>> rowData = new ArrayList<>();
                while (rs.next()) {
                    List<StorageValue> data = new ArrayList<>();
                    for (int i = 0; i < keyColumns.size(); i++) {
                        data.add(new StorageValue(keyColumns.get(i), rs.getObject(keyColumns.get(i)), columnTypes.get(i)));
                    }
                    rowData.add(data);
                }

                rowData.parallelStream().forEach(data -> {
                    SerializedData serializedData = new SerializedData();
                    serializedData.fromQuery(data);
                    try {
                        SimpleStorageModel model = new SimpleStorageModel(getTableStructure().getKeyName());
                        K key = serializedData.applyAs(model.getKey().toString(), keyClass);
                        for (Map.Entry<String, Object> entry : serializedData.getValues().entrySet()) {
                            try {
                                model.addValue(entry.getKey(), entry.getValue());
                            } catch (Exception e) {
                                DataDebugLog.logError("Failed to build simple storage model: " + e.getMessage());
                            }
                        }
                        modelMap.put(key, model);
                    } catch (Exception exception) {
                        DataDebugLog.logError("Failed to build simple storage model");
                        DataDebugLog.logError(exception.getMessage());
                    }
                });

            } catch (SQLException e) {
                DataDebugLog.logError("Failed to build Model Map of users: " + e.getMessage());
            }

            future.complete(modelMap);
            return null;
        }, executor));
        return future;
    }


    /**
     * Retrieves a sorted list of {@link SimpleStorageModel} objects from the database based on a specified column.
     * This method constructs a SQL query to fetch and sort records according to the given column, order, limit, and offset.
     *
     * @param databaseStructure The structure of the database table, defining column metadata.
     * @param sortByColumnName  The name of the column to sort the results by.
     * @param sortOrder         The sorting order, either {@link SortOrder#ASC} or {@link SortOrder#DESC}.
     * @param limit             The maximum number of records to retrieve.
     * @param offset            The number of records to skip before retrieving results.
     * @param async             Whether to execute the query asynchronously.
     * @return A {@link CompletableFuture} containing a list of sorted {@link SimpleStorageModel} objects.
     */
    @Override
    public CompletableFuture<List<SimpleStorageModel>> getSortedListByColumn(DatabaseStructure databaseStructure, String sortByColumnName, SortOrder sortOrder, int limit, int offset, boolean async) {
        Executor executor = getExecutor(async);

        CompletableFuture<List<SimpleStorageModel>> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>(conn -> {
            List<SimpleStorageModel> sortedList = new ArrayList<>();

            StringBuilder builder = new StringBuilder();
            builder.append("SELECT ");

            for (String key : databaseStructure.getColumnStructure().keySet()) {
                builder.append(key).append(",");
            }
            builder.setCharAt(builder.length() - 1, ' ');
            builder
                    .append("FROM ")
                    .append(credential.getTableName())
                    .append(" ORDER BY ")
                    .append(sortByColumnName)
                    .append(" ")
                    .append(sortOrder == SortOrder.DESC ? "DESC" : "ASC")
                    .append(" LIMIT ")
                    .append(limit)
                    .append(" OFFSET ")
                    .append(offset);

            try (Statement stmt = conn.createStatement()) {
                ResultSet resultSet = stmt.executeQuery(builder.toString());
                while (resultSet.next()) {
                    SimpleStorageModel model = buildSimpleStorageModel(resultSet, databaseStructure);
                    sortedList.add(model);
                }
            } catch (Exception e) {
                DataDebugLog.logError("Failed to build sorted list: " + e.getMessage());
            }
            future.complete(sortedList);
            return sortedList;
        }, executor));

        return future;
    }

    private SimpleStorageModel buildSimpleStorageModel(ResultSet rs, DatabaseStructure databaseStructure) throws SQLException {
        SimpleStorageModel model = new SimpleStorageModel(rs.getString(databaseStructure.getKeyName()));
        try {
            for (Map.Entry<String, ColumnData<?>> entry : databaseStructure.getColumnStructure().entrySet()) {
                String colName = entry.getKey();
                Object colVal = rs.getObject(colName);
                model.addValue(colName, colVal);
            }
        } catch (Exception e) {
            DataDebugLog.logError("Failed to build simple storage model: " + e.getMessage());

        }
        return model;
    }

    public enum SortOrder {
        ASC, DESC
    }

/*    private DatabaseStructure buildNeedsUpdate(T object, DatabaseStructure structure, SerializedData data) {


    }
    */


    private DatabaseStructure buildNeedsUpdateFromStruct(T object, DatabaseStructure startingStruct, SerializedData data) {

        if (startingStruct == null) {
            return buildNeedsUpdate(object, data);
        }
        ModelCachedData cachedData = getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());
        DatabaseStructure needsUpdate = new DatabaseStructure();
        needsUpdate.setTableName(credential.getTableName());
        DatabaseStructure dataStruct = data.toDatabaseStructure(getTableStructure());
        Map<String, ColumnData<?>> entries = dataStruct.getColumnStructure ();
        needsUpdate.addValue(dataStruct.getKeyName(), (SQLColumnData<?>) entries.get(dataStruct.getKeyName()));

        for (String key : startingStruct.getColumnStructure().keySet()) {
            SQLColumnData<?> columnData = (SQLColumnData<?>) entries.get(key);

            if (!columnData.isCompareCache()) {
                continue;
            }
            if (!data.getValue(key).isPresent() || cachedData.isOutdated(key, columnData.getValueOrDefault().toString())) {
                needsUpdate.addValue(key, columnData);
                DataDebugLog.logDebug(DataDebugLogType.SQL_UPDATE, getDataClass().getSimpleName() + " Database: Needs Update: " + key + " " + columnData.getValueOrDefault());
            }
        }
        return needsUpdate;

    }

    private DatabaseStructure buildNeedsUpdate(T object, SerializedData data) {
        ModelCachedData cachedData = getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());

        // Check the cache to see if there's outdated data.
        DatabaseStructure needsUpdate = new DatabaseStructure();
        needsUpdate.setTableName(credential.getTableName());
        boolean first = true;
        for (Map.Entry<String, ColumnData<?>> entry : data.toDatabaseStructure(getTableStructure()).getColumnStructure().entrySet()) {
            String column = entry.getKey();
            SQLColumnData<?> columnData = (SQLColumnData<?>) entry.getValue();
            // String value = entry.getValue().getValue() == null ? entry.getValue().getDefaultValue().toString() : entry.getValue().getValue().toString();

            if (first) {
                needsUpdate.addValue(column, columnData);
                first = false;
                continue;
            }
            if (!columnData.isCompareCache()) {
                continue;
            }
            if (!data.getValue(entry.getKey()).isPresent() || cachedData.isOutdated(column, columnData.getValueOrDefault().toString())) {
                needsUpdate.addValue(column, columnData);
                DataDebugLog.logDebug(DataDebugLogType.SQL_UPDATE, getDataClass().getSimpleName() + " Database: Needs Update: " + column + " " + columnData.getValueOrDefault());
            }
        }
        return needsUpdate;
    }

    protected boolean verifyColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnData<?>>> addColumns) {
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
            for (Map.Entry<String, ColumnData<?>> entry : getTableStructure().getColumnStructure().entrySet()) {
                if (!databaseColumns.containsKey(entry.getKey())) {
                    SQLColumnData<?> columnData = (SQLColumnData<?>) entry.getValue();
                    Map.Entry<String, SQLColumnData<?>> castedEntry = new AbstractMap.SimpleEntry<>(entry.getKey(), columnData);
                    addColumns.put(structureColumns.get(addCurrent - 1), castedEntry);
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

                SQLColumnData<?> columnData = (SQLColumnData<?>) getTableStructure().getColumnStructure().get(dataEntry.getKey());
                SQLColumnType structureColumnType = columnData.getColumnType();

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
        DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, getDataClass().getSimpleName() + " Database: Table Needs Alter: " + needsAltering);

        return needsAltering;
    }

    private static List<String> getDataMirrorArray(Map<String, Map.Entry<String, SQLColumnData<?>>> addColumns, Map<String, DataEntry<String, String>> dataClone, List<String> structureColumns) {
        List<String> dataMirrorArray = new ArrayList<>(dataClone.keySet());

        for (Map.Entry<String, Map.Entry<String, SQLColumnData<?>>> colEntry : addColumns.entrySet()) {
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

    // Cache existing entries for fast lookup
    private Map<Object, Boolean> cacheExistingEntries(Connection connection) throws SQLException {
        Map<Object, Boolean> existingEntries = new HashMap<>();
        String keyColumn = getTableStructure().getKeyName();
        try (PreparedStatement checkStmt = connection.prepareStatement("SELECT " + keyColumn + " FROM " + credential.getTableName())) {
            ResultSet rs = checkStmt.executeQuery();
            while (rs.next()) {
                Object object = StorageUtil.fromObject(rs.getObject(keyColumn), getKeyClass());
                DataDebugLog.logDebug(DataDebugLogType.ALL_SQL, "caching existing entries: " + object.toString());

                existingEntries.put(object, true);
            }
        }
        return existingEntries;
    }

    public <S> void executeSQLRequest(ConnectionRequest<S> request) {
        executeRequest(request);
    }

    public abstract String createTableStatement(boolean force);

    protected abstract void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnData<?>>> addColumns);

    public abstract String insertStatement(DatabaseStructure modifiedStructure);

    public abstract String updateStatement(DatabaseStructure modifiedStructure);

    public abstract void dropTable();

}
