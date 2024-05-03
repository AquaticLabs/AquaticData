package io.aquaticlabs.aquaticdata.model;

import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import lombok.Getter;

/**
 * @Author: extremesnow
 * On: 3/19/2024
 * At: 19:20
 */
@Getter
public class StorageValue {

    private final String field;
    private final Object value;
    private final SQLColumnType sqlColumnType;

    public StorageValue(String field, Object value, SQLColumnType sqlColumnType) {
        this.field = field;
        this.value = value;
        this.sqlColumnType = sqlColumnType;
    }

}
