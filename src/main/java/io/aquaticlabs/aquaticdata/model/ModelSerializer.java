package io.aquaticlabs.aquaticdata.model;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * @Author: extremesnow
 * On: 3/29/2024
 * At: 06:23
 */
public class ModelSerializer<T extends StorageModel> implements Serializer<T> {

    private BiConsumer<T, SerializedData> serializerFunction;
    private BiFunction<T, SerializedData, T> deserializerFunction;

    public ModelSerializer() {
    }

    public ModelSerializer<T> serializer(BiConsumer<T, SerializedData> serializer) {
        this.serializerFunction = serializer;
        return this;
    }

    public ModelSerializer<T> deserializer(BiFunction<T, SerializedData, T> deserializer) {
        this.deserializerFunction = deserializer;
        return this;
    }

    @Override
    public void serialize(T model, SerializedData serializedData) {
        if (serializerFunction != null) {
            serializerFunction.accept(model, serializedData);
        }
    }

    @Override
    public T deserialize(T model, SerializedData serializedData) {
        if (deserializerFunction != null) {
            return deserializerFunction.apply(model, serializedData);
        }
        return null;
    }
}
