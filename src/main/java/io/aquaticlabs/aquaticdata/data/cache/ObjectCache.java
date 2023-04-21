package io.aquaticlabs.aquaticdata.data.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.data.storage.Storage;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

/**
 * @Author: extremesnow
 * On: 8/25/2022
 * At: 19:07
 */
public class ObjectCache {

    private Storage<?> holder;
    @Getter
    private LoadingCache<Object, DataObject> objectCache;

    public ObjectCache(Storage<?> holder, long amount, TimeUnit unit) {
        this.holder = holder;

        CacheLoader<Object, DataObject> cache = new CacheLoader<Object, DataObject>() {
            @Override
            public DataObject load(Object key) {
                return null;
            }
        };
        objectCache = CacheBuilder.newBuilder()
                .maximumSize(500)
                .expireAfterWrite(amount, unit)
                .removalListener(defaultRemovalListener())
                .build(cache);

    }


    private RemovalListener<Object, DataObject> defaultRemovalListener() {
        return notification -> {
            DataDebugLog.logDebug("Going to remove data from InputDataPool");
            if (notification.getCause() == RemovalCause.EXPIRED) {
                DataDebugLog.logDebug("This data expired: " + notification.getKey());
                holder.removeDataObj(notification.getValue());
            } else {
                DataDebugLog.logDebug("This data was manually removed: " + notification.getKey());
            }
        };
    }

}
