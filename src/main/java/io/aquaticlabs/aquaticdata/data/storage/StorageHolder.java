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
import io.aquaticlabs.aquaticdata.data.type.mysql.MySQLDB;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
    private StorageMode storageMode = StorageMode.LOAD_AND_TIMEOUT;

    @Getter
    private final TaskFactory taskFactory = TaskFactory.getOrNew("StorageHolder<T> Factory");

    @Getter
    private ObjectCache cache;
    @Getter
    @Setter
    protected CacheMode cacheMode = CacheMode.TIME;

    @Getter
    protected RepeatingTask cacheSaveTask;
    @Getter
    private long cacheTimeInSecondsToSave = (60L * 5);


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
        addVariant(this.database.getTable(), clazz);
        initStorageMode(storageMode);

        long startTime = System.currentTimeMillis();

        confirmTable(t, false);

        initCacheMode(cacheMode);

        long endTime = System.currentTimeMillis();
        DataDebugLog.logDebug("Confirm Table: " + (endTime - startTime) + "ms");
    }


    protected void initStorageMode(StorageMode storageMode) {
        this.storageMode = storageMode;
        if (storageMode == StorageMode.LOAD_AND_TIMEOUT) {
            cache = new ObjectCache(this, timeOutTime, TimeUnit.MINUTES);
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
        cacheSaveTask.cancel();
        if (cacheMode == CacheMode.SHUTDOWN) {
            // I probably should just do this anyway? shouldn't be a specific feature?
            //saveLoaded(false, () -> DataDebugLog.logDebug("Saved Loaded on Shutdown."));
        }
        saveLoaded(false, () -> DataDebugLog.logDebug("SaveLoaded on Shutdown."));
        database.shutdown();
        taskFactory.shutdown();
    }

    @Override
    public void invalidateCacheEntryIfMode(T object) {
        if (storageMode == StorageMode.LOAD_AND_TIMEOUT) {
            cache.getObjectCache().invalidate(object);
        }
    }

    @Override
    protected void addToCache(T object) {
        switch (storageMode) {
            case LOAD_AND_TIMEOUT:
                cache.getObjectCache().put(object.getKey(), object);
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
            getCache().getObjectCache().cleanUp();
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
                        DataDebugLog.logDebug("Failed to deserialize class, with data: " + serializedData.toString());
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
            getCache().getObjectCache().cleanUp();
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
        DataDebugLog.logDebug("Save Loaded");

        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);

        database.getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(conn -> {

            int modified = 0;
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
                        DataDebugLog.logDebug("Execute Update Statement");
                        conn.createStatement().executeUpdate(database.buildUpdateStatementSQL(needsUpdate));
                    } catch (SQLException e) {
                        DataDebugLog.logDebug("Fail Updating Data: " + e.getMessage());
                    }
                } else {
                    try {
                        DataDebugLog.logDebug("Execute Insert Statement");
                        conn.createStatement().executeUpdate(database.insertStatement(columnList));
                    } catch (SQLException e) {
                        DataDebugLog.logDebug("Fail Inserting Data: " + e.getMessage());
                    }
                }
            }
            DataDebugLog.logDebug("Saved Loaded, Modified " + modified + " users.");
            if (run != null) {
                DataDebugLog.logDebug("Running callback");
                runner.accept(run);
            }
            return true;
        }, runner));
    }

    private List<DataEntry<String, String>> buildNeedsUpdate(T object, SerializedData data) {
        ModelCachedData cache = getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());

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
        Map<Integer, String> needsChange = new HashMap<>();
        Consumer<Runnable> runner = AquaticDatabase.getInstance().getRunner(async);
        String tempTableName = "AquaticDataTempTable";
        DataDebugLog.logDebug("confirm table?");


        database.getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(conn -> {

            try {
                DatabaseMetaData metaData = conn.getMetaData();


                ResultSet result = metaData.getColumns(null, null, database.getTable(), null);


                int columnCount = 0;
                while (result.next()) {
                    columnCount++;
                    String columnName = result.getString("COLUMN_NAME");
                    String columnTypeName = result.getString("TYPE_NAME");
                    // Use the column type name here

                    DataDebugLog.logDebug("ColumnName:" + columnName + " ColumnType: " + columnTypeName);

                    ColumnType colType = ColumnType.matchType(columnTypeName);


                    if (colType == null) {
                        needsChange.put(columnCount, columnName);
                        continue;
                    }

                    if (!ColumnType.isSimilarMatching(object.getStructure().get(columnCount - 1).getValue(), colType)) {
                        needsChange.put(columnCount, columnName);
                    }
                }


                if (columnCount != object.getStructure().size()) {
                    needsChange.put(1, "dummy");
                }


            } catch (Exception e) {
                throw new IllegalStateException("Failed to confirm table, strange Column Types/Names.", e);
            }



/*
            try (PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + database.getTable() + ";")) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    int cols = resultSet.getMetaData().getColumnCount();

                    if (cols != object.getStructure().size()) {
                        needsChange.put(1, "dummy");
                    }

                    for (int curCol = 1; curCol < cols; curCol++) {


                        ColumnType colType = ColumnType.matchType(resultSet.getMetaData().getColumnTypeName(curCol));
                        String colName = resultSet.getMetaData().getColumnTypeName(curCol);

                        DataDebugLog.logDebug(colName);

                        if (colType == null) {
                            needsChange.put(curCol, resultSet.getMetaData().getColumnName(curCol));
                            continue;
                        }

                        if (!ColumnType.isSimilarMatching(object.getStructure().get(curCol - 1).getValue(), colType)) {
                            needsChange.put(curCol, resultSet.getMetaData().getColumnName(curCol));
                        }
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to get Database Size", e);
            }
*/

            DataDebugLog.logDebug(needsChange);

            if (!needsChange.isEmpty()) {

                if (database instanceof MySQLDB) {
                    for (Map.Entry<Integer, String> e : needsChange.entrySet()) {


                        String stmt = "ALTER TABLE " + database.getTable() + " MODIFY COLUMN `" + e.getValue() + "` " + object.getStructure().get(e.getKey()).getValue().getSql() + " NOT NULL DEFAULT '0';;";
                        DataDebugLog.logDebug(stmt);

                        try (PreparedStatement preparedStatement = conn.prepareStatement(stmt)) {


                            preparedStatement.executeUpdate();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            throw new IllegalStateException("Failed to Alter Table.", ex);
                        }
                    }
                    return null;
                }
                // otherwise annoyingly annoying table copying


                String dropConflict = "DROP TABLE IF EXISTS " + tempTableName + ";";

                try (PreparedStatement preparedStatement = conn.prepareStatement(dropConflict)) {
                    preparedStatement.executeUpdate();
                } catch (Exception ex) {
                    DataDebugLog.logDebug("Failed to drop if exists Table. " + ex.getMessage());
                }

                String stmt1 = "ALTER TABLE " + database.getTable() + " RENAME TO " + tempTableName + ";";
                DataDebugLog.logDebug(stmt1);

                try (PreparedStatement preparedStatement = conn.prepareStatement(stmt1)) {
                    preparedStatement.executeUpdate();
                } catch (Exception ex) {
                    DataDebugLog.logDebug("Failed to Alter Table. " + ex.getMessage());
                }


                ArrayList<DataEntry<String, ColumnType>> structure = object.getStructure();
                database.createTable(structure, true);
                DataDebugLog.logDebug("copy");

                try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tempTableName)) {

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

                        try {
                            conn.createStatement().executeUpdate(database.insertStatement(serializedData.toColumnList(dummy.getStructure())));
                            DataDebugLog.logDebug("Success Inserting New Data");
                        } catch (SQLException e) {
                            DataDebugLog.logDebug("Fail Inserting New Data: " + e.getMessage());
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                String dropStmt = "DROP TABLE '" + tempTableName + "'";
                try (PreparedStatement preparedStatement = conn.prepareStatement(dropStmt)) {
                    preparedStatement.executeUpdate();
                } catch (Exception ex) {
                    DataDebugLog.logDebug("Failed to Drop temp Table. " + ex.getMessage());
                }
            }
            return null;
        }, runner));

        if (!needsChange.isEmpty()) {
            DataDebugLog.logDebug("table altered, moving to copying");
            DataDebugLog.logDebug("Needs: " + needsChange);
            return;
        }
        DataDebugLog.logDebug("table unchanged.");
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
