package io.aquaticlabs.aquaticdata.type.sql;

import com.google.common.annotations.VisibleForTesting;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.cache.ModelCachedData;
import io.aquaticlabs.aquaticdata.model.SerializedData;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
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
    private int batchSize = 500; // Adjust the batch size as needed

    protected SQLDatabase(SQLCredential credential, DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        super(tableStructure, serializer, asyncExecutor, syncExecutor);
        this.credential = credential;
    }


    public <K> void start(Storage<K, T> holder) {
        start(holder, true);
    }

    public <K> void start(Storage<K, T> holder, boolean async) {
        confirmTable(getTableStructure());
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
     *
     * @throws SQLException If an error occurs during batch execution or database operations.
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

                        DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Saving Loaded:" + object.getKey());

                        SerializedData data = new SerializedData();
                        getSerializer().serialize(object, data);


                        DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);

                        // If the size is 1, it should only contain the key.
                        if (needsUpdate.getColumnValues().size() == 1) {
                            DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Needs update contains no data values. no need for updating");
                            continue;
                        }


                        DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());
                        boolean exists = existingEntries.containsKey(object.getKey());

                        DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: exists: " + exists);

                        if (exists) {
                            try {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Adding Update Batch Statement");
                                statement.addBatch(updateStatement(needsUpdate));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Failed adding batch Data: " + e.getMessage());
                                continue;
                            }
                        } else {
                            try {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Adding Insert Batch Statement");
                                statement.addBatch(insertStatement(modifiedStructure));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Fail Inserting Data: " + e.getMessage());
                                continue;
                            }
                        }
                        modified++;

                        if (modified % batchSize == 0) {
                            try {
                                statement.executeBatch();
                                statement.clearBatch();
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Success executing batch of " + modified);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Failed executing batch: " + e.getMessage());
                            }
                        }
                    }
                    DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Executing Last batch of " + modified);
                    statement.executeBatch();
                    statement.clearBatch();
                    connection.commit(); // Commit the transaction
                } catch (SQLException e) {
                    connection.rollback();
                }
                connection.setAutoCommit(true);

                future.complete(saved);
                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Saved Loaded, Modified " + modified + " users.");
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
     *
     * @throws SQLException If an error occurs during batch execution or database operations.
     */
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

                        DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Saving List: " + object.getKey());

                        SerializedData data = new SerializedData();
                        getSerializer().serialize(object, data);

                        DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);

                        // If the size is 1, it should only contain the key.
                        if (needsUpdate.getColumnValues().size() == 1) {
                            DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Needs update contains no data values. no need for updating");
                            continue;
                        }

                        modified++;
                        DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());
                        if (doesEntryExist(connection, modifiedStructure.getFirstValuePair())) {

                            try {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Adding Update Batch Statement");
                                statement.addBatch(updateStatement(needsUpdate));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Failed adding batch Data: " + e.getMessage());
                            }
                        } else {
                            try {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Adding Insert Batch Statement");
                                statement.addBatch(insertStatement(modifiedStructure));
                                saved.add(object);
                            } catch (SQLException e) {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Fail Inserting Data: " + e.getMessage());
                            }
                        }


                        if (modified % batchSize == 0) {
                            try {
                                statement.executeBatch();
                                statement.clearBatch();

                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Success executing batch of " + modified);

                            } catch (SQLException e) {
                                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Failed executing batch: " + e.getMessage());
                            }
                        }
                    }
                    DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Executing Last batch of " + modified);
                    statement.executeBatch();

                    connection.commit(); // Commit the transaction
                } catch (SQLException e) {
                    connection.rollback();
                }
                connection.setAutoCommit(true);

                future.complete(saved);
                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Saved List, Modified " + modified + " users.");
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
     *
     * @throws SQLException If an error occurs while executing the SQL insert or update statement.
     */
    @Override
    public CompletableFuture<T> save(T object, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<T> future = new CompletableFuture<>();

        SerializedData data = new SerializedData();
        getSerializer().serialize(object, data);

        DatabaseStructure needsUpdate = buildNeedsUpdate(object, data);
        // If the size is 1, it should only contain the key.
        if (needsUpdate.getColumnValues().size() == 1) {
            DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Needs update contains no data values. no need for updating");
            return null;
        }

        getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(connection -> {
            DatabaseStructure modifiedStructure = data.toDatabaseStructure(getTableStructure());

            if (doesEntryExist(connection, modifiedStructure.getFirstValuePair())) {
                try {
                    connection.createStatement().executeUpdate(updateStatement(needsUpdate));
                    DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Success Updating Data");
                } catch (SQLException e) {
                    DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Fail Updating Data: " + e.getMessage());
                }
            } else {
                try {
                    connection.createStatement().executeUpdate(insertStatement(modifiedStructure));
                    DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Success Inserting Data");
                } catch (SQLException e) {
                    DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Fail Inserting Data: " + e.getMessage());
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
     *
     * @throws SQLException If an error occurs while executing the SQL query.
     * @throws Exception    If deserialization of the query result fails.
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
                for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
                    data.add(new StorageValue(entry.getKey(), rs.getObject(column), SQLColumnType.matchType(rs.getMetaData().getColumnTypeName(column))));
                    column++;
                }

                DataDebugLog.logDebug(sql);
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
     *
     * @throws SQLException If an error occurs while executing the SQL query.
     * @throws Exception    If deserialization of query results fails.
     */
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

                    } catch (Exception exception) {
                        DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Failed to deserialize class, with data: " + serializedData);
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



    /**
     * Retrieves a list of objects from the database based on a specific key column and value.
     * This method queries the database for rows where the specified key column matches the given key value,
     * deserializes the results, and loads them into a list.
     *
     * @param keyColumn The column name to filter the query results.
     * @param keyValue  The value to match in the specified key column.
     * @param async     Whether to execute the query asynchronously.
     * @return A {@link CompletableFuture} containing a list of deserialized objects of type {@code T}.
     *
     * @throws SQLException If an error occurs while executing the SQL query.
     * @throws Exception    If deserialization of query results fails.
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
                for (Map.Entry<String, SQLColumnType> entry : getTableStructure().getColumnStructure().entrySet()) {
                    columnNames.add(entry.getKey());
                    columnTypes.add(SQLColumnType.matchType(entry.getValue().getSql()));
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
                        DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Failed to deserialize class, with data: " + serializedData);
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
     *
     * @throws Exception If an error occurs while executing the SQL query or processing the results.
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
            for (Map.Entry<String, SQLColumnType> entry : databaseStructure.getColumnStructure().entrySet()) {
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
                DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Needs Update: " + column + " " + value);
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
        DataDebugLog.logDebug(getDataClass().getSimpleName() + " Database: Table Needs Alter: " + needsAltering);

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

    // Cache existing entries for fast lookup
    private Map<Object, Boolean> cacheExistingEntries(Connection connection) throws SQLException {
        Map<Object, Boolean> existingEntries = new HashMap<>();
        String keyColumn = getTableStructure().getKeyName();
        try (PreparedStatement checkStmt = connection.prepareStatement("SELECT " + keyColumn + " FROM " + credential.getTableName())) {
            ResultSet rs = checkStmt.executeQuery();
            while (rs.next()) {
                existingEntries.put(StorageUtil.fromObject(rs.getObject(keyColumn), getKeyClass()), true);
            }
        }
        return existingEntries;
    }

    protected void executeBatchSafely(PreparedStatement statement, int count) {
        try {
            statement.executeBatch();
            statement.clearBatch();
            DataDebugLog.logDebug("Executed batch of " + count + " rows.");
        } catch (SQLException ex) {
            DataDebugLog.logDebug("Batch execution failed: " + ex.getMessage());
        }
    }

    public <S> void executeSQLRequest(ConnectionRequest<S> request) {
        executeRequest(request);
    }

    public abstract String createTableStatement(boolean force);

    protected abstract void correctColumns(Connection connection, Set<String> removeColumns, Map<String, SQLColumnType> retypeColumns, Map<String, String> moveColumns, Map<String, Map.Entry<String, SQLColumnType>> addColumns);

    public abstract String insertStatement(DatabaseStructure modifiedStructure);

    public abstract String updateStatement(DatabaseStructure modifiedStructure);

    public abstract void dropTable();

}
