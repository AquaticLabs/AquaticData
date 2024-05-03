package io.aquaticlabs.aquaticdata.model;

import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import io.aquaticlabs.aquaticdata.util.StorageUtil;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @Author: extremesnow
 * On: 11/12/2021
 * At: 21:43
 */
@Getter
public class SerializedData {

    private final HashMap<String, Object> values;
    private final HashMap<String, SQLColumnType> valueTypes;

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

    public SQLColumnType getColumnType(String field) {
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

    public SerializedData fromQuery(List<StorageValue> storageValues) {
        for (StorageValue storageValue : storageValues) {
            write(storageValue.getField(), storageValue.getValue());
            valueTypes.put(storageValue.getField(), storageValue.getSqlColumnType());
        }
        return this;
    }

    public DatabaseStructure toDatabaseStructure(DatabaseStructure tableStructure) {
        DatabaseStructure structure = new DatabaseStructure(tableStructure.getTableName());
        for (Map.Entry<String, SQLColumnType> entry : tableStructure.getColumnStructure().entrySet()) {
            structure.addValue(entry.getKey(), entry.getValue(), applyAs(entry.getKey(), String.class));
        }
        return structure;
    }

    @Override
    public String toString() {
        return "SerializedData{" +
                "values=" + values +
                '}';
    }
}
