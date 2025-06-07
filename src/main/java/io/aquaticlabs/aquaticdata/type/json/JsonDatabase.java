package io.aquaticlabs.aquaticdata.type.json;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.aquaticlabs.aquaticdata.Database;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.SerializedData;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.storage.Storage;
import io.aquaticlabs.aquaticdata.type.sql.SQLDatabase;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import lombok.NonNull;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class JsonDatabase<T extends StorageModel> extends Database<T> {


    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final JsonCredential credential;
    private Map<String, JsonObject> data;


    protected JsonDatabase(JsonCredential credential, DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        super(tableStructure, serializer, asyncExecutor, syncExecutor);
        this.credential = credential;
    }

    private synchronized void initFile() {
        System.out.println("Init file");
        File file = credential.getFile();
        if (!file.exists()) {
            data = new java.util.LinkedHashMap<>();
            return;
        }
        try (FileReader reader = new FileReader(file)) {
            Type type = new TypeToken<Map<String, JsonObject>>() {
            }.getType();
            data = gson.fromJson(reader, type);
            if (data == null) {
                data = new java.util.LinkedHashMap<>();
            }
        } catch (IOException e) {
            data = new java.util.LinkedHashMap<>();
        }
        System.out.println("Data Size: " + data.size() + " data: " + data);
    }

    private synchronized void saveData() {
        File file = credential.getFile();
        try (FileWriter writer = new FileWriter(file)) {
            gson.toJson(data, writer);
        } catch (IOException ignored) {
        }
    }

    @Override
    public <S> S executeRequest(ConnectionRequest<S> connectionRequest) {
        throw new UnsupportedOperationException("JSON database does not support connection requests");
    }

    @Override
    public <K> void start(Storage<K, T> holder) {
        start(holder, true);
    }

    @Override
    public <K> void start(Storage<K, T> holder, boolean async) {
        initFile();
    }

    @Override
    public void shutdown() {
        saveData();
    }

    @Override
    public CompletableFuture<Boolean> confirmTable(DatabaseStructure modifiedStructure) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public <S extends Iterable<T>> CompletableFuture<List<T>> saveLoaded(S data, boolean async) {
        return saveLoaded(data, async, true);
    }

    @Override
    public <S extends Iterable<T>> CompletableFuture<List<T>> saveLoaded(S data, boolean async, boolean useRunner) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        executor.execute(() -> {
            List<T> saved = new ArrayList<>();
            for (T object : data) {
                saveInternal(object);
                saved.add(object);
            }
            saveData();
            future.complete(saved);
        });
        return future;
    }

    @Override
    public CompletableFuture<List<T>> saveList(List<T> list, boolean async) {
        return saveLoaded(list, async);
    }

    private void saveInternal(T object) {
        SerializedData serializedData = new SerializedData();
        getSerializer().serialize(object, serializedData);
        JsonObject json = new JsonObject();
        for (Map.Entry<String, Object> entry : serializedData.getValues().entrySet()) {
            json.add(entry.getKey(), gson.toJsonTree(entry.getValue()));
        }
        data.put(object.getKey().toString(), json);
    }

    @Override
    public CompletableFuture<List<T>> getKeyedList(String key, String keyValue, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        executor.execute(() -> {
            List<T> list = new ArrayList<>();
            for (JsonObject obj : data.values()) {
                JsonElement element = obj.get(key);
                if (element != null && keyValue.equals(element.getAsString())) {
                    SerializedData sd = new SerializedData();
                    for (Map.Entry<String, JsonElement> e : obj.entrySet()) {
                        sd.write(e.getKey(), gson.fromJson(e.getValue(), Object.class));
                    }
                    T model = getSerializer().deserialize(null, sd);
                    list.add(model);
                }
            }
            future.complete(list);
        });
        return future;
    }

    @Override
    public CompletableFuture<List<SimpleStorageModel>> getSortedListByColumn(DatabaseStructure databaseStructure, String sortByColumnName, SQLDatabase.SortOrder sortOrder, int limit, int offset, boolean async) {
        throw new UnsupportedOperationException("Sorting not implemented for JSON database");
    }

    @Override
    public CompletableFuture<T> save(T object, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            saveInternal(object);
            saveData();
            future.complete(object);
        });
        return future;
    }

    @Override
    public <K> CompletableFuture<T> load(Storage<K, T> holder, DataEntry<String, K> key, boolean async) {
        return load(holder, key, async, false);
    }

    @Override
    public <K> CompletableFuture<T> load(Storage<K, T> holder, DataEntry<String, K> key, boolean async, boolean persist) {
        Executor executor = getExecutor(async);
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            JsonObject obj = data.get(key.getValue().toString());
            if (obj == null) {
                future.complete(null);
                return;
            }
            SerializedData sd = new SerializedData();
            for (Map.Entry<String, JsonElement> e : obj.entrySet()) {
                sd.write(e.getKey(), gson.fromJson(e.getValue(), Object.class));
            }
            T model = getSerializer().deserialize(null, sd);
            if (holder != null) holder.add(model, persist);
            future.complete(model);
        });
        return future;
    }

    @Override
    public <K> CompletableFuture<List<T>> loadAll(Storage<K, T> holder, boolean async) {
        Executor executor = getExecutor(async);
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        executor.execute(() -> {
            List<T> list = new ArrayList<>();
            System.out.println(data.values());
            for (JsonObject obj : data.values()) {
                SerializedData sd = new SerializedData();
                for (Map.Entry<String, JsonElement> e : obj.entrySet()) {
                    sd.write(e.getKey(), gson.fromJson(e.getValue(), Object.class));
                }
                T model = getSerializer().deserialize(null, sd);
                if (holder != null) holder.add(model, false);
                list.add(model);
            }
            future.complete(list);
        });
        return future;
    }


}
