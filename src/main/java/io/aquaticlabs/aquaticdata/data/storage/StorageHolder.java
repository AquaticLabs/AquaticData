package io.aquaticlabs.aquaticdata.data.storage;

import io.aquaticlabs.aquaticdata.AquaticDatabase;
import io.aquaticlabs.aquaticdata.data.ADatabase;
import io.aquaticlabs.aquaticdata.data.cache.ModelCachedData;
import io.aquaticlabs.aquaticdata.data.cache.ObjectCache;
import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.data.storage.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.data.tasks.AquaticRunnable;
import io.aquaticlabs.aquaticdata.data.tasks.RepeatingTask;
import io.aquaticlabs.aquaticdata.data.tasks.TaskFactory;
import io.aquaticlabs.aquaticdata.data.type.DataCredential;
import io.aquaticlabs.aquaticdata.data.type.mariadb.MariaDB;
import io.aquaticlabs.aquaticdata.data.type.mysql.MySQLDB;
import io.aquaticlabs.aquaticdata.data.type.sqlite.SQLiteDB;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.StorageUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @Author: extremesnow
 * On: 8/22/2022
 * At: 16:24
 */
public abstract class StorageHolder<T extends DataObject> extends Storage<T> {

    @Getter
    private ADatabase database;
    private Class<T> clazz;
    @Getter
    @Setter
    private StorageMode storageMode = StorageMode.LOAD_AND_STORE;

    @Getter
    private TaskFactory taskFactory;

    @Getter
    private ObjectCache<T> temporaryDataCache;
    @Getter
    @Setter
    protected CacheMode cacheMode = CacheMode.TIME;

    @Getter
    protected RepeatingTask cacheSaveTask;
    @Getter
    private long cacheTimeInSecondsToSave = (60L * 5);

    private final int standardBatchSize = 250; // Adjust the batch size as needed

    /**
     * The Time in Minutes data should timeout (only active when LOAD_AND_TIMEOUT storage mode is active)
     */
    @Getter
    @Setter
    private int timeOutTime = 1;


    protected StorageHolder(DataCredential dataCredential, Class<T> clazz, StorageMode storageMode, CacheMode cacheMode) {
        T t;
        try {
            t = constructorOf(clazz).newInstance();
        } catch (Exception c) {
            c.printStackTrace();
            DataDebugLog.logError("FAILED TO CREATE DUMMY (" + clazz.getName() + ") OBJECT. STOPPING BOOTUP!");
            return;
        }

        this.database = dataCredential.build(t);
        this.clazz = clazz;

        String splitName = clazz.getName().split("\\.")[clazz.getName().split("\\.").length - 1];
        this.taskFactory = TaskFactory.getOrNew("StorageHolder<T>(" + splitName + ") Factory");

        addVariant(this.database.getTable(), clazz);
        initStorageMode(storageMode);


        confirmTable(t, true);

        initCacheMode(cacheMode);
    }


    protected void initStorageMode(StorageMode storageMode) {
        this.storageMode = storageMode;
        if (storageMode == StorageMode.LOAD_AND_TIMEOUT) {
            temporaryDataCache = new ObjectCache<>(this, timeOutTime, TimeUnit.MINUTES);
        }
        if (storageMode == StorageMode.LOAD_AND_STORE) {

        }
    }

    public void setCacheSaveTime(long seconds) {
        cacheTimeInSecondsToSave = seconds;
        if (cacheSaveTask != null)
            cacheSaveTask.setOrResetInterval(cacheTimeInSecondsToSave);
    }


    private void initCacheMode(CacheMode cacheMode) {
        initCacheMode(cacheMode, getCacheTimeInSecondsToSave());
    }

    private void initCacheMode(CacheMode cacheMode, long saveInterval) {
        this.cacheMode = cacheMode;
        cacheTimeInSecondsToSave = saveInterval;
        if (cacheMode == CacheMode.TIME) {
            cacheSaveTask = getTaskFactory().createRepeatingTask(new AquaticRunnable() {
                @Override
                public void run() {
                    saveLoaded(true, () -> DataDebugLog.logDebug("Cache Saved"));
                    DataDebugLog.logDebug("TaskID: " + getTaskId() + " Task Owner: " + getOwnerID());
                }
            }, getCacheTimeInSecondsToSave());
        }
    }


    public void shutdown() {
        if (cacheSaveTask != null) {
            cacheSaveTask.cancel();
        }
        if (cacheMode == CacheMode.SHUTDOWN) {
            // I probably should just do this anyway? shouldn't be a specific feature?
            //saveLoaded(false, () -> DataDebugLog.logDebug("Saved Loaded on Shutdown."));
        }
        saveLoaded(false, () -> {
            DataDebugLog.logDebug("SaveLoaded on Shutdown.");
            database.shutdown();
            taskFactory.shutdown();
        }, false);
    }

    @Override
    public void invalidateCacheEntryIfMode(T object) {
        if (storageMode == StorageMode.LOAD_AND_TIMEOUT) {
            temporaryDataCache.getDataCache().invalidate(object);
        }
    }

    @Override
    protected void addToCache(T object) {
        switch (storageMode) {
            case LOAD_AND_TIMEOUT:
                temporaryDataCache.put(object);
                break;
            case LOAD_AND_STORE:
                break;
            case LOAD_AND_REMOVE:
                onRemove(object);
        }
    }

    @Override
    protected void cleanCache() {
        if (storageMode == StorageMode.LOAD_AND_TIMEOUT) {
            temporaryDataCache.getDataCache().cleanUp();
        }
    }

    private void loadIntoCache(T object, SerializedData data) {
        ModelCachedData cache = getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());
        for (DataEntry<String, String> entryList : data.toColumnList(object.getStructure())) {
            String column = entryList.getKey();
            String value = entryList.getValue();
            cache.add(column, value);
        }
    }

    public void save(T object, boolean async) {
        getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());
        if (cacheMode == CacheMode.TIME || cacheMode == CacheMode.SHUTDOWN) {
            //updateCache(object); //this is dirty.. technically I don't need to do anything if the caching time or shutdown saving is enabled.
            return;
        }
        saveSingle(object, async);
    }

    public void loadAll(boolean async) {
        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);

        database.executeNonLockConnection(new ConnectionRequest<>(conn -> {
            T dummy;
            try {
                dummy = construct(clazz);
            } catch (Exception e) {
                e.printStackTrace();
                DataDebugLog.logDebug("Failed to Construct <T>(" + clazz.getName() + ") Class");
                return null;
            }
            ArrayList<DataEntry<String, ColumnType>> structure = dummy.getStructure();

            try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + database.getTable())) {
                while (rs.next()) {
                    List<DataEntry<String, Object>> data = new LinkedList<>();
                    for (DataEntry<String, ColumnType> entry : structure) {
                        data.add(new DataEntry<>(entry.getKey(), rs.getObject(entry.getKey())));
                    }
                    SerializedData serializedData = new SerializedData();
                    serializedData.fromQuery(data);

                    try {
                        T dumb = construct(clazz);
                        dumb.deserialize(serializedData);
                        loadIntoCache(dumb, serializedData);
                        add(dumb);
                    } catch (Exception exception) {
                        DataDebugLog.logDebug("Failed to deserialize class, with data: " + serializedData);
                        exception.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, runner));

        //Select * from table
    }


    public void load(DataEntry<String, ?> key, boolean async) {
        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);

        database.executeNonLockConnection(new ConnectionRequest<>(conn -> {

            if (!doesEntryExist(conn, key)) {
                return false;
            }

            T dummy = construct(clazz);
            ArrayList<DataEntry<String, ColumnType>> structure = dummy.getStructure();


            try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + database.getTable() + " WHERE " + key.getKey() + " = '" + key.getValue() + "'")) {
                int i = 1;
                List<DataEntry<String, Object>> data = new LinkedList<>();
                for (DataEntry<String, ColumnType> entry : structure) {
                    data.add(new DataEntry<>(entry.getKey(), rs.getObject(i)));
                    i++;
                }

                DataDebugLog.logDebug("SELECT * FROM " + database.getTable() + " WHERE " + key.getKey() + " = '" + key.getValue() + "'");

                SerializedData serializedData = new SerializedData();
                serializedData.fromQuery(data);
                dummy.deserialize(serializedData);
                loadIntoCache(dummy, serializedData);
                add(dummy);


            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, runner));

        //Select * from table where uuid = 'uuid'
    }

    public T load(DataEntry<String, ?> key, boolean async, boolean persistent) {
        if (getStorageMode() == StorageMode.LOAD_AND_TIMEOUT) {
            temporaryDataCache.getDataCache().cleanUp();
        }
        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);

        T t;
        try {
            t = construct(clazz);
        } catch (Exception e) {
            e.printStackTrace();
            DataDebugLog.logDebug("Failed to Construct <T>(" + clazz.getName() + ") Class");
            return null;
        }
        ArrayList<DataEntry<String, ColumnType>> structure = t.getStructure();
        T dummy = construct();

        if (dummy == null) {
            return null;
        }

        database.executeNonLockConnection(new ConnectionRequest<>(conn -> {
            if (!doesEntryExist(conn, key)) {
                return null;
            }
            try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + database.getTable() + " WHERE " + key.getKey() + " = '" + key.getValue() + "'")) {
                rs.next();
                int i = 1;
                List<DataEntry<String, Object>> data = new LinkedList<>();
                for (DataEntry<String, ColumnType> entry : structure) {
                    data.add(new DataEntry<>(entry.getKey(), rs.getObject(i)));
                    i++;
                }
                SerializedData serializedData = new SerializedData();
                serializedData.fromQuery(data);
                dummy.deserialize(serializedData);

                if (!persistent) {
                    loadIntoCache(dummy, serializedData);
                }

                return dummy;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, runner));

        return dummy;
    }

    public void saveLoaded(boolean async) {
        saveLoaded(async, null);
    }


    public void saveList(List<T> list, boolean async, Runnable run) {
        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);


        database.getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(conn -> {
            for (T object : list) {

                SerializedData data = new SerializedData();
                object.serialize(data);
                List<DataEntry<String, String>> columnList = data.toColumnList(object.getStructure());

                List<DataEntry<String, String>> needsUpdate = buildNeedsUpdate(object, data);
                if (needsUpdate.isEmpty()) {
                    DataDebugLog.logDebug("Needs update is empty. no need for updating");
                    return needsUpdate;
                }
                if (doesEntryExist(conn, columnList.get(0))) {

                    try {
                        conn.createStatement().executeUpdate(database.buildUpdateStatementSQL(needsUpdate));
                    } catch (SQLException e) {
                        DataDebugLog.logDebug("Fail Updating Data: " + e.getMessage());
                    }
                } else {
                    try {
                        conn.createStatement().executeUpdate(database.insertStatement(columnList));
                    } catch (SQLException e) {
                        DataDebugLog.logDebug("Fail Inserting Data: " + e.getMessage());
                    }
                }
            }
            if (run != null) {
                runner.accept(run);

            }
            return null;
        }, runner));
    }

    public void saveLoaded(boolean async, Runnable run) {
        saveLoaded(async, run, true);
    }

    public void saveLoaded(boolean async, Runnable callback, boolean createTask) {
        DataDebugLog.logDebug("Save Loaded");

        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);

        database.getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(conn -> {

            int modified = 0;

            try (Statement statement = conn.createStatement()) {

                conn.setAutoCommit(false);
                try {
                    for (T object : this) {

                        SerializedData data = new SerializedData();
                        object.serialize(data);
                        List<DataEntry<String, String>> columnList = data.toColumnList(object.getStructure());

                        List<DataEntry<String, String>> needsUpdate = buildNeedsUpdate(object, data);
                        if (needsUpdate.isEmpty()) {
                            continue;
                        }
                        modified++;
                        if (doesEntryExist(conn, columnList.get(0))) {

                            try {
                                DataDebugLog.logDebug("Adding Update Batch Statement");
                                statement.addBatch(database.buildUpdateStatementSQL(needsUpdate));
                                //conn.createStatement().executeUpdate(database.buildUpdateStatementSQL(needsUpdate));
                            } catch (SQLException e) {
                                DataDebugLog.logDebug("Failed adding batch Data: " + e.getMessage());
                            }
                        } else {
                            try {
                                DataDebugLog.logDebug("Adding Insert Batch Statement");
                                statement.addBatch(database.insertStatement(columnList));
                            } catch (SQLException e) {
                                DataDebugLog.logDebug("Fail Inserting Data: " + e.getMessage());
                            }
                        }


                        if (modified % standardBatchSize == 0) {
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

                    conn.commit(); // Commit the transaction
                } catch (SQLException e) {
                    conn.rollback();
                }
                conn.setAutoCommit(true);


                DataDebugLog.logDebug("Saved Loaded, Modified " + modified + " users.");
                if (callback != null) {
                    DataDebugLog.logDebug("Running callback");
                    if (createTask) {
                        runner.accept(callback);
                    } else {
                        callback.run();
                    }
                }
                return true;
            }
        }, createTask ? runner : null));
    }

    private List<DataEntry<String, String>> buildNeedsUpdate(T object, SerializedData data) {
        ModelCachedData cache = getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());

        List<DataEntry<String, String>> needsUpdate = new ArrayList<>();
        for (DataEntry<String, String> entryList : data.toColumnList(object.getStructure())) {
            String columnName = entryList.getKey();
            String value = entryList.getValue();

            if (!data.getValue(columnName).isPresent()) {
                needsUpdate.add(new DataEntry<>(columnName, value));
                DataDebugLog.logDebug("Needs Update: " + columnName + " " + value);
                continue;
            }

            // DataDebugLog.logDebug("Cache Details: " + columnName + " Cache Value Hash: " + cache.getCache().get(columnName) + " New Value Hash: " + cache.hashString(value));

            if (!cache.isOutdated(columnName, value)) {
                // DataDebugLog.logDebug("Cache Is Up to date: " + columnName + " " + value);
                continue;
            }

            // DataDebugLog.logDebug("Cache Is Outdated... Needs Update: " + columnName + " Value: " + value);
            needsUpdate.add(new DataEntry<>(columnName, value));
        }
        if (needsUpdate.isEmpty()) {
            return needsUpdate;
        }
        String key = object.getStructure().get(0).getKey();
        String existingKey = needsUpdate.get(0).getKey();
        if (!existingKey.equalsIgnoreCase(key)) {
            Optional<Object> value = data.getValue(key);
            value.ifPresent(o -> needsUpdate.add(0, new DataEntry<>(key, o.toString())));
        }
        return needsUpdate;
    }

    public void saveSingle(T object, boolean async) {
        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);
        ModelCachedData cache = getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());
        SerializedData data = new SerializedData();
        object.serialize(data);
        // Column, Data
        List<DataEntry<String, String>> needsUpdate = new ArrayList<>();

        for (DataEntry<String, String> entryList : data.toColumnList(object.getStructure())) {
            String column = entryList.getKey();
            String value = entryList.getValue();

            if (!data.getValue(entryList.getKey()).isPresent()) {
                needsUpdate.add(new DataEntry<>(column, value));
                DataDebugLog.logDebug("Needs Update: " + column + " " + value);
                continue;
            }

            if (!cache.isOutdated(column, value)) continue;
            DataDebugLog.logDebug("Needs Update: " + column + " " + value);

            needsUpdate.add(new DataEntry<>(column, value));
        }
        if (needsUpdate.isEmpty()) {
            DataDebugLog.logDebug("Needs update is empty. no need for updating");
            return;
        }
        String key = object.getStructure().get(0).getKey();
        String existingKey = needsUpdate.get(0).getKey();
        if (!existingKey.equalsIgnoreCase(key)) {
            Optional<Object> value = data.getValue(key);
            value.ifPresent(o -> needsUpdate.add(0, new DataEntry<>(key, o.toString())));
        }


        database.getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(conn -> {

            List<DataEntry<String, String>> columnList = data.toColumnList(object.getStructure());

            if (doesEntryExist(conn, columnList.get(0))) {

                try {
                    conn.createStatement().executeUpdate(database.buildUpdateStatementSQL(needsUpdate));
                    DataDebugLog.logDebug("Success Updating Data");
                } catch (SQLException e) {
                    DataDebugLog.logDebug("Fail Updating Data: " + e.getMessage());
                }
            } else {
                try {
                    conn.createStatement().executeUpdate(database.insertStatement(columnList));
                    DataDebugLog.logDebug("Success Inserting Data");
                } catch (SQLException e) {
                    DataDebugLog.logDebug("Fail Inserting Data: " + e.getMessage());
                }
            }

            return true;
        }, runner));
    }

    public boolean doesEntryExist(Connection conn, DataEntry<String, ?> key) {
        String sql = "SELECT 1 FROM " + database.getTable() + " WHERE " + key.getKey() + " = ?";
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = conn.prepareStatement(sql);
            statement.setObject(1, key.getValue());
            resultSet = statement.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            throw new IllegalStateException("Error while checking if entry exists in database", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public void confirmTable(T object, boolean async) {
        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);
        String tempTableName = "AquaticDataTempTable";
        DataDebugLog.logDebug("confirm table?");
        final long startTime = System.currentTimeMillis();

        AtomicBoolean needsAltering = new AtomicBoolean(false);
        Set<String> removeColumns = new HashSet<>();
        Map<String, ColumnType> retypeColumns = new LinkedHashMap<>();
        Map<String, String> moveColumns = new LinkedHashMap<>();
        Map<String, DataEntry<String, ColumnType>> addColumns = new LinkedHashMap<>();

        database.executeNonLockConnection(new ConnectionRequest<>(conn -> {

            Map<String, DataEntry<String, ColumnType>> structureMap = new LinkedHashMap<>();
            for (DataEntry<String, ColumnType> entry : object.getStructure()) {
                structureMap.put(entry.getKey(), entry);
            }

            try {

                List<String> structureColumns = object.getStructure().stream().map(DataEntry::getKey).collect(Collectors.toList());

                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet result = metaData.getColumns(null, null, database.getTable(), null);

                Map<String, DataEntry<String, String>> databaseColumns = new LinkedHashMap<>();
                while (result.next()) {
                    String colName = result.getString("COLUMN_NAME");
                    databaseColumns.put(colName, new DataEntry<>(colName, result.getString("TYPE_NAME")));
                }

                // This column name isn't inside the structure
                for (String col : databaseColumns.keySet()) {
                    if (!structureColumns.contains(col)) {
                        removeColumns.add(col);
                        needsAltering.set(true);
                    }
                }


                int addCurrent = 0;
                for (DataEntry<String, ColumnType> entry : object.getStructure()) {
                    if (!databaseColumns.containsKey(entry.getKey())) {
                        addColumns.put(structureColumns.get(addCurrent - 1), entry);
                        needsAltering.set(true);
                    }
                    addCurrent++;
                }


                Map<String, DataEntry<String, String>> dataClone = new LinkedHashMap<>(databaseColumns);

                for (String col : removeColumns) {
                    dataClone.remove(col);
                }

                List<String> dataMirrorArray = new ArrayList<>(dataClone.keySet());

                for (Map.Entry<String, DataEntry<String, ColumnType>> colEntry : addColumns.entrySet()) {
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

                StorageUtil.calculateMoves(moveColumns, dataMirrorArray,structureColumns);
                if (!moveColumns.isEmpty()) {
                    needsAltering.set(true);
                }


                for (DataEntry<String, String> dataEntry : databaseColumns.values()) {
                    if (!structureMap.containsKey(dataEntry.getKey())) {
                        continue;
                    }
                    ColumnType structureColumnType = structureMap.get(dataEntry.getKey()).getValue();

                    String databaseColumnName = dataEntry.getKey();
                    String databaseColumnTypeName = dataEntry.getValue();
                    ColumnType databaseColType = ColumnType.matchType(databaseColumnTypeName);

                    // Column type is wrong
                    if (databaseColType == null || !ColumnType.isSimilarMatching(structureColumnType, databaseColType)) {
                        needsAltering.set(true);
                        retypeColumns.put(databaseColumnName, structureColumnType);
                    }
                }


            } catch (Exception e) {
                throw new IllegalStateException("Failed to confirm table, strange Column Types/Names.", e);
            }
            DataDebugLog.logDebug("Table Needs Alter: " + needsAltering);


            if (needsAltering.get()) {
                if (database instanceof MySQLDB || database instanceof MariaDB) {
                    List<String> batches = new ArrayList<>();


                    String alterStmt = "ALTER TABLE " + database.getTable();

                    // First up: removes

                    for (String col : removeColumns) {
                        batches.add(alterStmt + " DROP COLUMN " + col + ";");
                    }

                    // Next is adds

                    for (Map.Entry<String, DataEntry<String, ColumnType>> columnEntry : addColumns.entrySet()) {
                        batches.add(alterStmt + " ADD " + columnEntry.getValue().getKey() + " " + columnEntry.getValue().getValue().getSql() + " NOT NULL AFTER " + columnEntry.getKey() + ";");
                    }

                    // Now moves

                    for (Map.Entry<String, String> moveEntry : moveColumns.entrySet()) {
                        batches.add(alterStmt + " CHANGE " + moveEntry.getKey() + " " + moveEntry.getKey() + " " + structureMap.get(moveEntry.getKey()).getValue().getSql() + " NOT NULL AFTER " + moveEntry.getValue() + ";");
                    }

                    // Lastly Type Changes
                    for (Map.Entry<String, ColumnType> retypeEntry : retypeColumns.entrySet()) {
                        batches.add(alterStmt + " MODIFY COLUMN `" + retypeEntry.getKey() + "` " + retypeEntry.getValue().getSql() + " NOT NULL DEFAULT '0';");
                    }

                    try (Statement statement = conn.createStatement()) {

                        for (String batch : batches) {
                            statement.addBatch(batch);
                        }
                        statement.executeBatch();
                        DataDebugLog.logDebug("Success executing alter table batches.");

                    } catch (Exception ex) {
                        DataDebugLog.logDebug("Failed to Alter Table. " + ex.getMessage());
                    }

                    return null;
                }


                if (database instanceof SQLiteDB) {
                    // otherwise annoyingly annoying table copying
                    String dropConflict = "DROP TABLE IF EXISTS " + tempTableName + ";";

                    try (PreparedStatement preparedStatement = conn.prepareStatement(dropConflict)) {
                        preparedStatement.executeUpdate();
                    } catch (Exception ex) {
                        DataDebugLog.logDebug("Failed to drop if exists Table. " + ex.getMessage());
                    }


                    String stmt1 = "ALTER TABLE " + database.getTable() + " RENAME TO " + tempTableName + ";";
                    DataDebugLog.logDebug(stmt1);
                    PreparedStatement preparedStatement = conn.prepareStatement(stmt1);
                    try {
                        preparedStatement.executeUpdate();
                        DataDebugLog.logDebug("Success renaming table.");

                    } catch (Exception ex) {
                        DataDebugLog.logDebug("Failed to Alter Table. " + ex.getMessage());
                        return null;
                    }


                    ArrayList<DataEntry<String, ColumnType>> structure = object.getStructure();
                    database.createTable(structure, true);
                    DataDebugLog.logDebug("copy");

                    try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tempTableName)) {
                        conn.setAutoCommit(false); // Start a transaction

                        Statement statement = conn.createStatement();

                        int count = 0;
                        T dummy = construct();

                        while (rs.next()) {

                            List<DataEntry<String, Object>> data = new LinkedList<>();

                            for (DataEntry<String, ColumnType> entry : structure) {
                                try {
                                    rs.findColumn(entry.getKey());
                                } catch (SQLException sql) {
                                    data.add(new DataEntry<>(entry.getKey(), object.getDefaultDataValue(entry.getKey())));
                                    continue;
                                }
                                data.add(new DataEntry<>(entry.getKey(), rs.getObject(entry.getKey())));
                            }

                            SerializedData serializedData = new SerializedData();
                            serializedData.fromQuery(data);
                            assert dummy != null;
                            dummy.deserialize(serializedData);

                            String insertStatement = database.insertStatement(serializedData.toColumnList(dummy.getStructure()));
                            statement.addBatch(insertStatement);
                            count++;

                            if (count % standardBatchSize == 0) {
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


                        conn.commit(); // Commit the transaction
                    } catch (SQLException e) {
                        conn.rollback();
                        e.printStackTrace();
                    }

                    String dropStmt = "DROP TABLE '" + tempTableName + "'";
                    try (PreparedStatement dropStatement = conn.prepareStatement(dropStmt)) {
                        dropStatement.executeUpdate();

                    } catch (Exception ex) {
                        DataDebugLog.logDebug("Failed to Drop temp Table. " + ex.getMessage());
                    }
                }
            }
            conn.setAutoCommit(true); // Start a transaction

            return true;
        }, runner).whenComplete(() -> {
            long endTime = System.currentTimeMillis();
            DataDebugLog.logDebug("Confirmed Table:  " + database.getTable() + " took " + (endTime - startTime) + "ms");
            if (needsAltering.get()) {
                DataDebugLog.logDebug("Table altered");
                DataDebugLog.logDebug("Needed:");
                DataDebugLog.logDebug("Removes: " + removeColumns);
                DataDebugLog.logDebug("Adds: " + addColumns.values().stream().map(DataEntry::getKey).collect(Collectors.toList()));
                DataDebugLog.logDebug("Moves: " + moveColumns.keySet());
                DataDebugLog.logDebug("Retypes: " + retypeColumns.keySet());
                return;
            }
            DataDebugLog.logDebug("table unchanged.");

        }));
    }




    private T construct() {
        T dummy;
        try {
            dummy = constructorOf(clazz).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            DataDebugLog.logDebug("Failed to Construct Dummy Class <T>(" + clazz.getName() + ") Class");
            return null;
        }
        return dummy;
    }


    public int getDataSize(Connection conn) {
        String sql = "SELECT COUNT(*) FROM " + database.getTable();
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = conn.prepareStatement(sql);
            resultSet = statement.executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Error while checking if entry exists in database", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    @SneakyThrows
    public <B extends T> B construct(Class<B> clazz) {
        return (B) getConstructorMap()
                .computeIfAbsent(
                        clazz,
                        key -> {
                            try {
                                Constructor<B> constructor = clazz.getDeclaredConstructor();
                                constructor.setAccessible(true);
                                return constructor;
                            } catch (Exception exception) {
                                throw new IllegalStateException(
                                        "Failed to find empty constructor for " + clazz);
                            }
                        })
                .newInstance();
    }
}
