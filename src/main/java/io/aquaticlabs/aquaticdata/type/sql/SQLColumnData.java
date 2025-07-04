package io.aquaticlabs.aquaticdata.type.sql;

import io.aquaticlabs.aquaticdata.type.ColumnData;
import io.aquaticlabs.aquaticdata.util.StorageUtil;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * @Author: extremesnow
 * On: 7/3/2025
 * At: 18:23
 */
@Getter
public class SQLColumnData<V> extends ColumnData<V> {

    private final SQLColumnType columnType;

    public SQLColumnData(Class<V> valueClass) {
        super(StorageUtil.getDefaultValueFromClass(valueClass));
        this.columnType = SQLColumnType.matchColumnClassType(valueClass);
    }

    @SneakyThrows
    public SQLColumnData(V defaultValue) {
        super(defaultValue);
        this.columnType = SQLColumnType.matchColumnClassType(defaultValue.getClass());
    }
}
