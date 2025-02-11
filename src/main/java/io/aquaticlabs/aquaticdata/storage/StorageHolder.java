/*
 *
 *  * (c) Frosty Realms 2023-2024 by Project LXST LLC
 *  *
 *  * All rights reserved.
 *  *
 *  * This software is the confidential and proprietary information of Project LXST LLC ("Confidential Information").
 *  * Any and all Confidential Information shall only be used in accordance to any and all agreements entered between all parties involved and Project LXST LLC.
 *
 */

package io.aquaticlabs.aquaticdata.storage;

import io.aquaticlabs.aquaticdata.Database;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.tasks.AquaticRunnable;
import io.aquaticlabs.aquaticdata.tasks.TaskFactory;
import io.aquaticlabs.aquaticdata.type.DataCredential;
import io.aquaticlabs.aquaticdata.type.sql.SQLDatabase;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import lombok.NonNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 3/19/2024
 * At: 16:01
 */
public abstract class StorageHolder<K, T extends StorageModel> extends Storage<K, T> {

    private final Database<T> database;

    protected StorageHolder(DataCredential dataCredential, Class<K> keyClazz, Class<T> clazz, StorageMode storageMode, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        setKeyClass(keyClazz);

        String splitName = keyClazz.getName().split("\\.")[keyClazz.getName().split("\\.").length - 1];
        setTaskFactory(TaskFactory.getOrNew("StorageHolder<T>(" + splitName + ") Factory"));
        initStorageMode(storageMode);

        this.database = dataCredential.build(getStructure(), createSerializer(), asyncExecutor, syncExecutor);
        this.database.setKeyClass(keyClazz);
        this.database.setDataClass(clazz);
    }


    protected void loadDatabase() {
        initCacheMode(getCacheSaveMode(), getCacheTimeInSecondsToSave());
        database.start(this, false);
    }

    public void shutdown() {
        if (cacheSaveTask != null) {
            cacheSaveTask.cancel();
        }
        try {
            database.saveLoaded(this, false, false).whenComplete((v, t) -> {
                DataDebugLog.logDebug("SaveLoaded on Shutdown.");
                database.shutdown();
                getTaskFactory().shutdown();
            });
        } catch (Exception e) {
            DataDebugLog.logError("Failed To Save Loaded on shutdown");
        }
    }


    @Override
    protected void cleanCache() {
        if (storageMode == StorageMode.LOAD_AND_TIMEOUT || storageMode == StorageMode.CACHE) {
            temporaryDataCache.getDataCache().cleanUp();
        }
    }

    @Override
    protected void addToCache(T object) {
        switch (storageMode) {
            case CACHE:
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
    public void invalidateCacheEntryIfMode(T object) {
        if (getStorageMode() == StorageMode.LOAD_AND_TIMEOUT || getStorageMode() == StorageMode.CACHE) {
            getTemporaryDataCache().getDataCache().invalidate(object);
        }
    }

    private void initCacheMode(CacheSaveMode cacheSaveMode, long saveInterval) {
        setCacheSaveTime(saveInterval);
        if (cacheSaveMode == CacheSaveMode.TIME) {
            cacheSaveTask = getTaskFactory().createRepeatingTask(new AquaticRunnable() {
                @Override
                public void run() {
                    saveLoaded(true).whenComplete((users, t) -> DataDebugLog.logDebug("Cache Saved"));
                    DataDebugLog.logDebug("TaskID: " + getTaskId() + " Task Owner: " + getOwnerID());
                }
            }, getCacheTimeInSecondsToSave());
        }
    }

    protected CompletableFuture<List<T>> saveLoaded(boolean async) {
        return database.saveLoaded(this, async);
    }

    protected CompletableFuture<T> save(T object, boolean async) {
        return database.save(object, async);
    }

    protected CompletableFuture<List<T>> saveList(List<T> objects, boolean async) {
        return database.saveList(objects, async);
    }

    protected CompletableFuture<T> load(DataEntry<String, K> key, boolean async) {
        return database.load(this, key, async);
    }

    protected CompletableFuture<T> load(DataEntry<String, K> key, boolean async, boolean persist) {
        return database.load(this, key, async, persist);
    }

    protected CompletableFuture<List<T>> getKeyedList(String key, String keyValue, boolean async) {
        return database.getKeyedList(key, keyValue, async);
    }

    protected CompletableFuture<List<T>> loadAll(boolean async) {
        return database.loadAll(this, async);
    }

    protected CompletableFuture<List<SimpleStorageModel>> getSortedListByColumn(DatabaseStructure databaseStructure, String sortByColumnName, SQLDatabase.SortOrder sortOrder, int limit, int offset, boolean async) {
        return database.getSortedListByColumn(databaseStructure, sortByColumnName, sortOrder, limit, offset, async);
    }

    protected void executeRequest(ConnectionRequest<?> connectionRequest) {
        database.executeRequest(connectionRequest);
    }

    protected void dropTable() {
        if (database instanceof SQLDatabase) {
            SQLDatabase<?> db = (SQLDatabase<?>) database;
            db.dropTable();
        }
    }
}
