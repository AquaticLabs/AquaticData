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

@Getter
public abstract class Storage<K, T extends StorageModel> implements Iterable<T> {


    @Setter
    private Class<K> keyClass;

    @Setter
    private TaskFactory taskFactory;

    @Setter
    private StorageMode storageMode = StorageMode.LOAD_AND_STORE;
    @Setter
    private CacheMode cacheMode = CacheMode.TIME;

    private ObjectCache<K, T> temporaryDataCache;

    @Setter
    private long cacheTimeInSecondsToSave = (60L * 5);
    /**
     * The Time in Minutes data should timeout (only active when LOAD_AND_TIMEOUT storage mode is active)
     */
    @Setter
    private int timeOutTime = 1;

    protected RepeatingTask cacheSaveTask;

    public abstract DatabaseStructure getStructure();

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

    protected Constructor<T> constructorOf(Class<T> type) throws ConstuctorFailThrowable {
        Constructor<T> constructor;
        try {
            constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new ConstuctorFailThrowable("Please ensure that the type '" + type.getSimpleName() + "' has a single-arg constructor.");
        }
        return constructor;
    }

    public enum CacheMode {

        TIME,
        INSTANT,
        SHUTDOWN
    }
}
