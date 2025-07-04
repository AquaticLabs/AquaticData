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


    @SneakyThrows
    protected ColumnData(V defaultValue) {
        if (defaultValue == null) {
            throw new ConstructorFailThrowable();
        }
        this.defaultValue = defaultValue;
        this.value = defaultValue;
    }

    public V getValueOrDefault() {
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

}
