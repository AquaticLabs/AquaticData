package io.aquaticlabs.aquaticdata.data.storage;

import io.aquaticlabs.aquaticdata.data.object.StatObject;
import io.aquaticlabs.aquaticdata.data.type.DataCredential;


/**
 * @Author: extremesnow
 * On: 10/31/2023
 * At: 13:53
 */
public abstract class StatStorageHolder<T extends StatObject> extends StorageHolder<T>{

    public StatStorageHolder(DataCredential dataCredential, Class<T> clazz, StorageMode storageMode, CacheMode cacheMode) {
        super(dataCredential, clazz, storageMode, cacheMode);
    }




}
