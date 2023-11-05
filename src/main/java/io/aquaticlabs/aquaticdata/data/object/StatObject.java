package io.aquaticlabs.aquaticdata.data.object;

import io.aquaticlabs.aquaticdata.data.storage.ColumnType;

import java.util.ArrayList;

/**
 * @Author: extremesnow
 * On: 10/31/2023
 * At: 13:26
 */
public abstract class StatObject implements DataObject {

    protected abstract Object getDefaultStatValue(int statID);

    public ArrayList<DataEntry<String, ColumnType>> getStructure() {
        ArrayList<DataEntry<String, ColumnType>> structure = new ArrayList<>();
        structure.add(new DataEntry<>("stat_id", ColumnType.INTEGER));
        structure.add(new DataEntry<>("player_id", ColumnType.VARCHAR_UUID));
        structure.add(new DataEntry<>("stat_name", ColumnType.VARCHAR));
        structure.add(new DataEntry<>("stat_value", ColumnType.DOUBLE));
        structure.add(new DataEntry<>("stat_rank", ColumnType.INTEGER));
        return structure;
    }

}
