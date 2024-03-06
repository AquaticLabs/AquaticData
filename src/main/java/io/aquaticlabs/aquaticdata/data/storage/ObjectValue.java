package io.aquaticlabs.aquaticdata.data.storage;

import lombok.Getter;

/**
 * @Author: extremesnow
 * On: 3/6/2024
 * At: 17:08
 */
@Getter
public class ObjectValue {

    private final String field;
    private final Object value;
    private final ColumnType columnType;

    public ObjectValue(String field, Object value, ColumnType columnType) {
        this.field = field;
        this.value = value;
        this.columnType = columnType;
    }

}
