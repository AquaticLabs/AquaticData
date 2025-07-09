package testing.multidatatest.stat;

import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.ModelSerializer;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.storage.StorageHolder;
import io.aquaticlabs.aquaticdata.storage.StorageMode;
import io.aquaticlabs.aquaticdata.type.DataCredential;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnData;
import io.aquaticlabs.aquaticdata.util.DataEntry;

import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StatModelHolder extends StorageHolder<UUID, StatModel> {

    private int lastLoadedDataSize;

    public StatModelHolder(DataCredential credential) {
        super(credential, UUID.class, StatModel.class, StorageMode.LOAD_AND_REMOVE, CompletableFuture::runAsync, Runnable::run);
        loadDatabase();
    }

    public StatModel loadOrInsert(UUID uuid) {
        StatModel model;
        try {
            model = load(new DataEntry<>("uuid", uuid), false).get(1, TimeUnit.SECONDS);
            return model;
        } catch (Exception e) {
            e.printStackTrace();
        }
        model = new StatModel(uuid);
        return null;
    }

    public StatModel insert(StatModel statModel) {
        add(statModel);
        try {
            save(statModel, true).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return statModel;
    }


    @Override
    public DatabaseStructure getStructure() {
        DatabaseStructure structure = new DatabaseStructure();
        structure.addColumn("uuid", new SQLColumnData<>(UUID.class));
        structure.addColumn("name", new SQLColumnData<>(String.class));
        structure.addColumn("value", new SQLColumnData<>(0));
        return structure;
    }

    @Override
    protected void onAdd(StatModel object) {
        // Not needed, never holding modelObjects in memory.
    }

    @Override
    protected void onRemove(StatModel object) {
        // Not needed, never holding modelObjects in memory.
    }

    @Override
    public StatModel get(UUID key) {
        // Not needed, never holding modelObjects in memory.
        return null;
    }

    public void saveAll(boolean async) throws ExecutionException, InterruptedException, TimeoutException {
        super.saveLoaded(async).get(1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public Serializer<StatModel> createSerializer() {
        return new ModelSerializer<StatModel>().serializer((model, data) -> {
            data.write("uuid", model.getKey());
            data.write("name", model.getName());
            data.write("value", model.getValue());
        }).deserializer((model, data) -> {
            if (model == null) {
                model = new StatModel();
            }
            model.setUuidKey(data.applyAs("uuid", UUID.class));
            model.setName(data.applyAs("name", String.class));
            model.setValue(data.applyAs("value", Integer.class));
            return model;
        });
    }

    @Override
    public Iterator<StatModel> iterator() {
        return Collections.emptyIterator();
    }

    public void close() {
        shutdown();
    }
}
