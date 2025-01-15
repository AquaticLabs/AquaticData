package io.aquaticlabs.aquaticdata.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author: extremesnow
 * On: 1/5/2025
 * At: 19:55
 */
public class SimpleStorageModel implements StorageModel {

    private final Object key;
    private final Map<String, Object> valueMap = new LinkedHashMap<>();

    public SimpleStorageModel(Object key) {
        this.key = key;
    }

    public void addValue(String key, Object value) {
        valueMap.putIfAbsent(key, value);
    }

    public Object getValue(String key) {
        return valueMap.get(key) != null ? valueMap.get(key) : null;
    }

    @Override
    public Object getKey() {
        return key;
    }
}
