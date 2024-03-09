package io.aquaticlabs.aquaticdata.data.storage;

import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.util.StorageUtil;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * @Author: extremesnow
 * On: 11/12/2021
 * At: 21:43
 */
public class SerializedData {

    @Getter
    private final HashMap<String, Object> values;
    @Getter
    private final HashMap<String, ColumnType> valueTypes;

    public SerializedData() {
        this.values = new HashMap<>();
        this.valueTypes = new HashMap<>();
    }

    public <T> T applyAs(String key, Class<T> castingClass, Supplier<T> defaultValue) {
        return getValue(key)
                .map(element -> StorageUtil.fromObject(element, castingClass))
                .orElseGet(defaultValue == null ? () -> null : defaultValue);
    }

    public <T> T applyAs(String field, Class<T> clazz) {
        return applyAs(field, clazz, null);
    }

    public ColumnType getColumnType(String field) {
        return valueTypes.get(field);
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

    public SerializedData fromQuery(List<ObjectValue> objectValues) {
        for (ObjectValue objectValue : objectValues) {
            write(objectValue.getField(), objectValue.getValue());
            valueTypes.put(objectValue.getField(),objectValue.getColumnType());
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
