package io.aquaticlabs.aquaticdata;

import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 19:32
 */
@Getter
public class DatabaseStructure {

    private final Map<String, SQLColumnType> columnStructure = new LinkedHashMap<>();
    private final Map<String, Object> columnValues = new LinkedHashMap<>();
    private final Map<String, Object> columnDefaults = new LinkedHashMap<>();

    private String tableName;

    public DatabaseStructure() {}

    public DatabaseStructure(String tableName) {
        this.tableName = tableName;
    }


    public DatabaseStructure addColumn(String columnName, SQLColumnType sqlColumnType) {
        return addColumn(columnName, sqlColumnType, "");
    }

    public DatabaseStructure addColumn(String columnName, SQLColumnType sqlColumnType, Object defaultValue) {
        columnStructure.put(columnName, sqlColumnType);
        columnDefaults.put(columnName, defaultValue);
        return this;
    }

    public DatabaseStructure addValue(String columnName, SQLColumnType sqlColumnType, Object value) {
        columnStructure.put(columnName, sqlColumnType);
        columnValues.put(columnName, value);
        return this;
    }

    public DataEntry<String, String> getFirstValuePair() {
        Map.Entry<String, Object> entry = columnValues.entrySet().iterator().next();
        return new DataEntry<>(entry.getKey(), entry.getValue().toString());
    }

}
