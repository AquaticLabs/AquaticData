package io.aquaticlabs.aquaticdata.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.storage.Storage;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;

import java.util.concurrent.TimeUnit;

/**
 * @Author: extremesnow
 * On: 8/25/2022
 * At: 19:07
 */
public class ObjectCache<K, T extends StorageModel> {

    private final Storage<K, T> holder;

    private final Cache<Object, T> dataCache;

    public ObjectCache(Storage<K, T> holder, long duration, TimeUnit unit) {
        this.holder = holder;

        dataCache = CacheBuilder.newBuilder()
                .maximumSize(800)
                .expireAfterWrite(duration, unit)
                .removalListener(defaultRemovalListener())
                .build();
    }

    public void put(T value) {
        dataCache.put(value.getKey(), value);
    }

    public int size() {
        return (int) dataCache.size();
    }

    private RemovalListener<Object, T> defaultRemovalListener() {
        return notification -> {
            DataDebugLog.logDebug("Going to remove data from InputDataPool");
            if (notification.getCause() == RemovalCause.EXPIRED) {
                DataDebugLog.logDebug("This data expired: " + notification.getKey());
                holder.remove(notification.getValue());
            } else {
                DataDebugLog.logDebug("This data was manually removed: " + notification.getKey());
            }
        };
    }

    public Cache<Object, T> getDataCache() {
        return dataCache;
    }
}
