package io.aquaticlabs.aquaticdata.type;

import io.aquaticlabs.aquaticdata.util.ConstructorFailThrowable;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

/**
 * @Author: extremesnow
 * On: 7/3/2025
 * At: 19:41
 */
@Getter
@Setter

public abstract class ColumnData<V> {

    protected V value;
    protected V defaultValue;
    protected boolean compareCache = true;


    @SneakyThrows
    protected ColumnData(V defaultValue) {
        if (defaultValue == null) {
            throw new ConstructorFailThrowable();
        }
        this.defaultValue = defaultValue;
        this.value = defaultValue;
    }
    @SneakyThrows
    protected ColumnData(V defaultValue, boolean compareCache) {
        if (defaultValue == null) {
            throw new ConstructorFailThrowable();
        }
        this.defaultValue = defaultValue;
        this.value = defaultValue;
        this.compareCache = compareCache;
    }

    protected ColumnData<V> compareCache(boolean compareCache) {
        this.compareCache = compareCache;
        return this;
    }

    public V getValueOrDefault() {
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

}
