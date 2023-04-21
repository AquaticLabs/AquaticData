package io.aquaticlabs.aquaticdata.data.object;

import io.aquaticlabs.aquaticdata.data.storage.ColumnType;

import java.util.ArrayList;

/**
 * @Author: extremesnow
 * On: 4/10/2022
 * At: 19:33
 */
public interface DataObject extends SerializableObject, Saveable {

    Object getKey();

    Object getDefaultDataValue(String columnName);

    ArrayList<DataEntry<String, ColumnType>> getStructure();

}
