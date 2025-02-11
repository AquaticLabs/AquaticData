package io.aquaticlabs.aquaticdata.model;

/**
 * @Author: extremesnow
 * On: 3/29/2024
 * At: 06:37
 */
public interface Serializer <T extends StorageModel>{

    T deserialize(T loadedUser, SerializedData data);

    void serialize(T object, SerializedData data);

}
