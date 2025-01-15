package testing;

import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.ModelSerializer;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.storage.StorageHolder;
import io.aquaticlabs.aquaticdata.storage.StorageMode;
import io.aquaticlabs.aquaticdata.type.DataCredential;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnType;
import io.aquaticlabs.aquaticdata.type.sql.SQLDatabase;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import testing.newtest.LogData;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: extremesnow
 * On: 11/3/2024
 * At: 23:55
 */
public class TestHolder extends StorageHolder<UUID, TestData> {

    private Map<UUID, TestData> dataMap = new ConcurrentHashMap<>();
    private DataCredential credential;

    public TestHolder(DataCredential credential) {
        super(credential, UUID.class, TestData.class, StorageMode.CACHE, CompletableFuture::runAsync, Runnable::run);
        this.credential = credential;
        setCacheSaveTime(60L * 10);
        setCacheSaveMode(CacheSaveMode.TIME);
        setCacheTimeOutTime(5);
        loadDatabase();
    }

    public TestData getOrNull(UUID uuid) {
        return dataMap.get(uuid);
    }


    public TestData create(TestData data) {
        if (dataMap.containsKey(data.getKey())) {
            return data;
        }
        add(data, true);
       // save(data, true);
        return data;
    }

    public TestData getOrCreate(UUID uuid) {
        if (dataMap.containsKey(uuid)) {
            return dataMap.get(uuid);
        }
        try {
            TestData data = load(uuid);
            if (data == null) {
                // Data doesn't exist, make it.
                data = new TestData(uuid);
                onAdd(data);
                save(data, true);
                return data;
            }

            return data;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public void closeOut(UUID uuid) {
        TestData data = dataMap.get(uuid);
        save(data, true);
        remove(data);
    }

    public TestData loadIntoCache(UUID uuid) {
        if (dataMap.containsKey(uuid)) {
            return dataMap.get(uuid);
        }
        try {
            return load(uuid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public TestData load(UUID uuid) throws ExecutionException, InterruptedException, TimeoutException {
        return load(new DataEntry<>("uuid", uuid), false).get(1000, TimeUnit.MILLISECONDS);
    }

    public void cleanUp() {
        cleanCache();
    }

    @Override
    public DatabaseStructure getStructure() {
        DatabaseStructure structure = new DatabaseStructure();
        structure.addColumn("uuid", SQLColumnType.VARCHAR_UUID);
        structure.addColumn("name", SQLColumnType.VARCHAR_64);
        structure.addColumn("value", SQLColumnType.INTEGER, 0);
        structure.addColumn("value_rank", SQLColumnType.INTEGER, 0);


        return structure;
    }

    @Override
    protected void onAdd(TestData object) {
        dataMap.put(object.getKey(), object);
    }

    @Override
    protected void onRemove(TestData object) {
        dataMap.remove(object.getKey());
    }

    @Override
    public TestData get(UUID key) {
        return dataMap.get(key);
    }

    @Override
    public Serializer<TestData> createSerializer() {
        return new ModelSerializer<TestData>().serializer((model, data) -> {
            data.write("uuid", model.getKey());
            data.write("name", model.getName());
            data.write("value", model.getValue());
        }).deserializer((model, data) -> {
            if (model == null) {
                model = new TestData();
            }
            model.setKey(data.applyAs("uuid", UUID.class));
            model.setName(data.applyAs("name", String.class));
            model.setValue(data.applyAs("value", Integer.class));
            return model;
        });
    }


    public CompletableFuture<List<SimpleStorageModel>> getSortedDataList(String sortColumn) {
        DatabaseStructure structure = new DatabaseStructure();
        structure.addColumn("uuid", SQLColumnType.VARCHAR_UUID);
        structure.addColumn("name", SQLColumnType.VARCHAR_64);
        structure.addColumn("value", SQLColumnType.INTEGER);

        return super.getSortedListByColumn(structure, sortColumn, SQLDatabase.SortOrder.DESC, 50, 0, true);
    }

    public void loadRanks(String statValue) throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>((connection -> {
            try (PreparedStatement statement = connection.prepareStatement(buildUpdateRankQuery(statValue))) {
                statement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            future.complete(true);
            return null;
        }), CompletableFuture::runAsync));
        // force thread to wait for outcome.
        future.get(10, TimeUnit.SECONDS);
    }

    public int getRank(UUID uuid) throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        executeRequest(new ConnectionRequest<>((connection -> {
            AtomicInteger result = new AtomicInteger();
            try (PreparedStatement statement = connection.prepareStatement("SELECT value_rank FROM " + credential.getTableName() + " WHERE uuid='" + uuid + "'")) {
                ResultSet rs = statement.executeQuery();
                result.set(rs.getInt(1));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            future.complete(result.intValue());
            return null;
        }), Runnable::run));
        return future.get(10, TimeUnit.SECONDS);
    }

    private String buildUpdateRankQuery(String columnName) {
        String tableName = credential.getTableName();

        return String.format("WITH cte AS (SELECT *, ROW_NUMBER() OVER (ORDER BY %s DESC) rn FROM %s) " +
                "UPDATE %s SET %s_rank = (SELECT rn FROM cte c WHERE (c.uuid, c.%s_rank) = (%s.uuid, %s.%s_rank))",
                columnName, tableName, tableName, columnName, columnName, tableName, tableName, columnName);
    }


    public void saveAll(boolean async) throws ExecutionException, InterruptedException, TimeoutException {
        super.saveLoaded(async).get(1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public Iterator<TestData> iterator() {
        return dataMap.values().iterator();
    }

    public void close() {
        shutdown();
    }
}
