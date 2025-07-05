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
    public SQLColumnData(Class<V> valueClass, boolean compareCache) {
        super(StorageUtil.getDefaultValueFromClass(valueClass), compareCache);
        this.columnType = SQLColumnType.matchColumnClassType(valueClass);
    }

    public SQLColumnData<V> compareCache(boolean compareCache) {
        this.compareCache = compareCache;
        return this;
    }

    @SneakyThrows
    public SQLColumnData(V defaultValue) {
        super(defaultValue);
        this.columnType = SQLColumnType.matchColumnClassType(defaultValue.getClass());
    }
}
