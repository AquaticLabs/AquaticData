package io.aquaticlabs.aquaticdata.data.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.data.storage.Storage;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: extremesnow
 * On: 8/25/2022
 * At: 19:07
 */
public class ObjectCache<V extends DataObject> {

    private final Storage<V> holder;

    private final Cache<Integer, V> cache;
    private final AtomicInteger counter;

    public ObjectCache(Storage<V> holder, long duration, TimeUnit unit) {
        this.holder = holder;

        cache = CacheBuilder.newBuilder()
                .maximumSize(500)
                .expireAfterWrite(duration, unit)
                .removalListener(defaultRemovalListener())
                .build();
        counter = new AtomicInteger();
    }

    public void put(V value) {
        int key = counter.getAndIncrement();
        cache.put(key, value);
    }

    public int size() {
        return (int)cache.size();
    }

    private RemovalListener<Integer, V> defaultRemovalListener() {
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
}
