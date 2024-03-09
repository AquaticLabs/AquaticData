package testing;

import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.data.storage.CacheMode;
import io.aquaticlabs.aquaticdata.data.storage.ColumnType;
import io.aquaticlabs.aquaticdata.data.storage.StorageHolder;
import io.aquaticlabs.aquaticdata.data.storage.StorageMode;
import io.aquaticlabs.aquaticdata.data.type.DataCredential;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: extremesnow
 * On: 8/24/2022
 * At: 21:16
 */
public class TestHold extends StorageHolder<TestData> {

    @Getter
    private Map<UUID, TestData> data = new ConcurrentHashMap<>();

    public TestHold(DataCredential dataCredential) {
        super(dataCredential, TestData.class, StorageMode.LOAD_AND_STORE, CacheMode.TIME);
        setCacheSaveTime(10L);
        loadAll(false);
    }

    public TestData getOrNull(UUID uuid) {
        return data.get(uuid);
    }

    public TestData getOrInsert(TestData dataObj, boolean persistent) {
        if (data.containsKey(dataObj.uuid)) {
            return data.get(dataObj.uuid);
        }
        add(dataObj, persistent);
        return dataObj;
    }

    @Override
    public List<DataEntry<String, ColumnType>> getStructure() {
        ArrayList<DataEntry<String, ColumnType>> structure = new ArrayList<>();
        structure.add(new DataEntry<>("uuid", ColumnType.VARCHAR_UUID));
        structure.add(new DataEntry<>("name", ColumnType.VARCHAR));
        structure.add(new DataEntry<>("level", ColumnType.INT));
        structure.add(new DataEntry<>("stat1", ColumnType.INT));
        structure.add(new DataEntry<>("stat2", ColumnType.INT));
        structure.add(new DataEntry<>("stat3", ColumnType.INT));
        return structure;    }

    public TestData getOrInsert(TestData dataObj) {
        return getOrInsert(dataObj, true);
    }

    @Override
    protected void onAdd(TestData object) {
        data.put(object.uuid, object);
    }

    @Override
    protected void onRemove(TestData object) {
        data.remove(object.uuid, object);
    }

    @Override
    public Iterator<TestData> iterator() {
        return data.values().iterator();
    }

}
