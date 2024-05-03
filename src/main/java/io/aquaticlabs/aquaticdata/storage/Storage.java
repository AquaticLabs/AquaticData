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
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.util.ConstuctorFailThrowable;
import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.Constructor;

@Getter
public abstract class Storage<K, T extends StorageModel> implements Iterable<T> {


    @Setter
    private Class<K> keyClass;


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
}
