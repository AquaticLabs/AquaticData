package testing.newtest;

import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.ModelSerializer;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.storage.StorageHolder;
import io.aquaticlabs.aquaticdata.storage.StorageMode;
import io.aquaticlabs.aquaticdata.type.DataCredential;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import io.aquaticlabs.aquaticdata.util.DataEntry;

import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @Author: extremesnow
 * On: 5/13/2023
 * At: 06:21
 */
public class LogDataHolder extends StorageHolder<Integer, LogData> {

    //private final Map<UUID, List<LogData>> data = new ConcurrentHashMap<>();
    //private final Map<Integer, LogData> data = new ConcurrentHashMap<>();
    //private final Map<UUID, List<LogData>> userMap = new ConcurrentHashMap<>();


    public LogDataHolder(DataCredential credential) {
        super(credential, Integer.class, LogData.class, StorageMode.LOAD_AND_STORE, CompletableFuture::runAsync, Runnable::run);
        setCacheSaveTime(60L * 10);
        setCacheSaveMode(CacheSaveMode.TIME);
        setStorageMode(StorageMode.LOAD_AND_REMOVE);
    }


    public void saveNewLogData(LogData logData) {
        save(logData, true);
    }

    public void loadUserLogData(UUID uuid) {
        //todo
    }

    public CompletableFuture<LogData> loadLogData(Integer alertId) {
        return load(new DataEntry<>(getStructure().getFirstValuePair().getKey(), alertId), true);
    }

    public CompletableFuture<List<LogData>> loadUUIDData(UUID uuid) {
        return getKeyedList("uuid", uuid.toString(), false);
    }

/*    public List<LogData> getOrNull(UUID uuid) {
        return userMap.get(uuid);
    }*/

    public void insertLogData(LogData logData) {
        onAdd(logData);
    }

    @Override
    protected void onAdd(LogData object) {
        // not needed
/*        data.put(object.getAlertID(), object);
        if (userMap.get(object.getPlayerUUID()) == null) {
            userMap.put(object.getPlayerUUID(), new ArrayList<>(Collections.singletonList(object)));
            return;
        }
        userMap.get(object.getPlayerUUID()).add(object);*/
    }

    @Override
    protected void onRemove(LogData object) {
        // not needed
/*        data.remove(object.getAlertID());
        if (userMap.get(object.getPlayerUUID()) == null) return;
        List<LogData> logData = userMap.get(object.getPlayerUUID());
        logData.removeIf(object::isEqual);
        userMap.put(object.getPlayerUUID(), logData);*/
    }

    @Override
    public LogData get(Integer key) {
        // not needed
        return null;//data.get(key);
    }

    public void callSQLRequest(ConnectionRequest<?> connectionRequest) {
        executeRequest(connectionRequest);
    }

/*
    public List<LogData> getAllLogData() {
        return new ArrayList<>(data.values());
*/
/*        ArrayList<LogData> allData = new ArrayList<>();
        for (List<LogData> logData : userMap.values()) {
            allData.addAll(logData);
        }
        return allData;*//*

    }
*/

    @Override
    public Serializer<LogData> createSerializer() {
        return new ModelSerializer<LogData>()
                .serializer((model, serializedData) -> {
                    serializedData.write("alertID", model.getAlertID());
                    serializedData.write("timestamp", model.getTimestamp());
                    serializedData.write("uuid", model.getPlayerUUID());
                    serializedData.write("name", model.getPlayerName());
                    serializedData.write("checkType", model.getCheckType().name());
                    serializedData.write("moduleName", model.getModuleName());
                    serializedData.write("checkData", Utils.encodeByteArrayToBase64(model.getCheckData().compressJsonToBytes()));
                }).deserializer((model, serializedData) -> {
                    if (model == null) {
                        model = new LogData();
                    }
                    model.setAlertID(serializedData.applyAs("alertID", Integer.class));
                    model.setTimestamp(serializedData.applyAs("timestamp", Long.class));
                    model.setPlayerUUID(serializedData.applyAs("uuid", UUID.class));
                    model.setPlayerName(serializedData.applyAs("name", String.class));
                    model.setCheckType(CheckType.matchType(serializedData.applyAs("checkType", String.class)));
                    model.setModuleName(serializedData.applyAs("moduleName", String.class));
                    model.setCheckData(CheckData.decompressJsonFromBytes(Utils.getBytesFromBase64(serializedData.applyAs("checkData", String.class))));
                    return model;
                });
    }

    @Override
    public DatabaseStructure getStructure() {
        DatabaseStructure structure = new DatabaseStructure();
        structure.addColumn("alertID", SQLColumnType.INTEGER);
        structure.addColumn("timestamp", SQLColumnType.LONG);
        structure.addColumn("uuid", SQLColumnType.VARCHAR_UUID);
        structure.addColumn("name", SQLColumnType.VARCHAR);
        structure.addColumn("checkType", SQLColumnType.VARCHAR);
        structure.addColumn("moduleName", SQLColumnType.TEXT);
        structure.addColumn("checkData", SQLColumnType.TEXT);
        return structure;
    }

    @Override
    public Iterator<LogData> iterator() {
        return new ArrayList<LogData>().iterator(); //getAllLogData().iterator();
    }

    public void close() {
        shutdown();
    }
}
