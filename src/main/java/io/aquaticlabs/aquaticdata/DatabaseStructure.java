package io.aquaticlabs.aquaticdata;

import io.aquaticlabs.aquaticdata.type.ColumnData;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnData;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 19:32
 */
@Getter
public class DatabaseStructure {

    private String keyName;
    private final Map<String, ColumnData<?>> columnStructure = new LinkedHashMap<>();

    @Setter
    private String tableName;

    public DatabaseStructure() {
    }

/*

    public DatabaseStructure addColumn(String columnName, SQLColumnType sqlColumnType) {
        return addColumn(columnName, new SQLColumnData<>(sqlColumnType));
    }
*/

    public DatabaseStructure addColumn(String columnName, SQLColumnData<?> columnData) {
        if (keyName == null) {
            keyName = columnName;
        }
        columnStructure.put(columnName, columnData);
        return this;
    }

    public DatabaseStructure addValue(String columnName, SQLColumnData<?> columnData) { //SQLColumnType sqlColumnType, Object value) {
        columnStructure.put(columnName, columnData);
        return this;
    }

    public DataEntry<String, String> getFirstValuePair() {
        Map.Entry<String, ColumnData<?>> entry = columnStructure.entrySet().iterator().next();
        return new DataEntry<>(entry.getKey(), entry.getValue().getValue().toString());
    }

}
