package testing;

import io.aquaticlabs.aquaticdata.data.object.DataObject;
import io.aquaticlabs.aquaticdata.data.storage.CacheMode;
import io.aquaticlabs.aquaticdata.data.storage.StorageHolder;
import io.aquaticlabs.aquaticdata.data.storage.StorageMode;
import io.aquaticlabs.aquaticdata.data.type.DataCredential;
import lombok.Getter;

import java.util.Iterator;
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
