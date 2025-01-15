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

import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.cache.ObjectCache;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.tasks.RepeatingTask;
import io.aquaticlabs.aquaticdata.tasks.TaskFactory;
import io.aquaticlabs.aquaticdata.util.ConstuctorFailThrowable;
import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

@Getter
public abstract class Storage<K, T extends StorageModel> implements Iterable<T> {

    @Setter
    private Class<K> keyClass;

    @Setter
    private TaskFactory taskFactory;

    @Setter
    private long cacheTimeInSecondsToSave = (60L * 5);
    /**
     * The Time in Minutes data should timeout (only active when LOAD_AND_TIMEOUT storage mode is active)
     */
    private int timeOutTime = 60;

    protected RepeatingTask cacheSaveTask;

    @Setter
    protected StorageMode storageMode = StorageMode.LOAD_AND_STORE;
    @Setter
    private Storage.CacheSaveMode cacheSaveMode = Storage.CacheSaveMode.TIME;

    protected ObjectCache<K, T> temporaryDataCache;

    public abstract DatabaseStructure getStructure();

    protected void initStorageMode(StorageMode storageMode) {
        this.storageMode = storageMode;
        if (storageMode == StorageMode.LOAD_AND_TIMEOUT || storageMode == StorageMode.CACHE) {
            temporaryDataCache = new ObjectCache<>(this, timeOutTime, TimeUnit.SECONDS);
        }
        if (storageMode == StorageMode.LOAD_AND_STORE) {

        }
    }

    protected void setCacheTimeOutTime(int timeOutTime) {
        this.timeOutTime = timeOutTime;
        if (temporaryDataCache != null) {
            temporaryDataCache.setOrResetInterval(timeOutTime);
        }
    }


    protected abstract void onAdd(T object);

    protected abstract void onRemove(T object);

    public void add(T object, boolean hold) {
        cleanCache();
        onAdd(object);
        if (!hold) {
            addToCache(object);
        }
    }

    protected abstract void addToCache(T object);

    public void add(T object) {
        add(object, false);
    }

    public void remove(T object) {
        cleanCache();
        onRemove(object);
        invalidateCacheEntryIfMode(object);
    }

    public abstract void invalidateCacheEntryIfMode(T object);

    protected abstract void cleanCache();

    public void setCacheSaveTime(long seconds) {
        cacheTimeInSecondsToSave = seconds;
        if (cacheSaveTask != null)
            cacheSaveTask.setOrResetInterval(cacheTimeInSecondsToSave);
    }

    public abstract T get(K key);

    public abstract Serializer<T> createSerializer();

    public enum CacheSaveMode {

        TIME,
        INSTANT,
        SHUTDOWN
    }
}
