package io.aquaticlabs.aquaticdata.data.storage;

import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.util.StorageUtil;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @Author: extremesnow
 * On: 11/12/2021
 * At: 21:43
 */
public class SerializedData {

    @Getter
    private final HashMap<String, Object> values;

    public SerializedData() {
        this.values = new HashMap<>();
    }

    public <T> T applyAs(String key, Class<T> castingClass, Supplier<T> defaultValue) {
        return getValue(key)
                .map(element -> StorageUtil.fromObject(element, castingClass))
                .orElseGet(defaultValue == null ? () -> null : defaultValue);
    }

    public <T> T applyAs(String field, Class<T> clazz) {
        return applyAs(field, clazz, null);
    }



    public Optional<Object> getValue(String field) {
        Object element = values.get(field);
        return (element == null)
                ? Optional.empty()
                : Optional.of(element);
    }

    public void write(String field, Object object) {
        values.put(field, object);
    }

    public SerializedData fromQuery(List<DataEntry<String, Object>> objectValues) {
        for (DataEntry<String, Object> objectValue : objectValues) {
            write(objectValue.getKey(), objectValue.getValue());
        }
        return this;
    }

    public List<DataEntry<String, String>> toColumnList(List<DataEntry<String, ColumnType>> columns) {
        List<DataEntry<String, String>> data = new ArrayList<>();
        for (DataEntry<String, ColumnType> entry : columns) {
            data.add(new DataEntry<>(entry.getKey(), applyAs(entry.getKey(), String.class)));
        }
        return data;
    }

}
