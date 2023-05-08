package io.aquaticlabs.aquaticdata.data.storage;

import io.aquaticlabs.aquaticdata.data.cache.ModelCachedData;
import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.util.ConstuctorFailThrowable;
import lombok.AccessLevel;
import lombok.Getter;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: extremesnow
 * On: 4/12/2022
 * At: 22:25
 */
public abstract class Storage<T extends DataObject> implements Iterable<T> {

    @Getter
    private final Map<String, Class<T>> variants = new HashMap<>();

    @Getter(value = AccessLevel.PROTECTED)
    private final Map<Class<? extends T>, Constructor<? extends T>> constructorMap = new ConcurrentHashMap<>();


    @Getter(value = AccessLevel.PROTECTED)
    private final Map<String, ModelCachedData> dataCache = new ConcurrentHashMap<>();



    public void addVariant(String variant, Class<T> clazz) {
        variants.put(variant, clazz);
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

    public abstract void removeDataObj(DataObject key);

    protected abstract void cleanCache();


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
}
