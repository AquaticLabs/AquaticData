package io.aquaticlabs.aquaticdata;

import io.aquaticlabs.aquaticdata.cache.ModelCachedData;
import io.aquaticlabs.aquaticdata.model.SerializedData;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.queue.ConnectionQueue;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.storage.Storage;
import io.aquaticlabs.aquaticdata.type.sql.SQLDatabase;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 18:31
 */
@Getter
public abstract class Database<T extends StorageModel> {

    private final DatabaseStructure tableStructure;
    private final ConnectionQueue connectionQueue;
    private final Serializer<T> serializer;

    @NonNull
    @Setter
    @Getter
    private Class<?> keyClass;

    @NonNull
    @Getter
    @Setter
    private Class<T> dataClass;

    @Getter(value = AccessLevel.PROTECTED)
    private final Map<String, ModelCachedData> dataCache = new ConcurrentHashMap<>();

    @Getter(value = AccessLevel.PROTECTED)
    private final Map<Class<? extends T>, Constructor<? extends T>> constructorMap = new ConcurrentHashMap<>();

    private final Executor asyncExecutor;
    private final Executor syncExecutor;

    protected Database(DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        this.tableStructure = tableStructure;
        this.serializer = serializer;
        this.asyncExecutor = asyncExecutor;
        this.syncExecutor = syncExecutor;
        connectionQueue = new ConnectionQueue(this);
    }

    protected Executor getExecutor(boolean async) {
        return async ? asyncExecutor : syncExecutor;
    }

    protected void loadIntoCache(T object, SerializedData data) {
        ModelCachedData cache = getDataCache().computeIfAbsent(object.getKey().toString(), key -> new ModelCachedData());

        for (Map.Entry<String, Object> entryList : data.toDatabaseStructure(getTableStructure()).getColumnValues().entrySet()) {
            String column = entryList.getKey();
            String value = entryList.getValue().toString();
            cache.add(column, value);
        }
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public <B extends T> B construct(Class<B> clazz) {
        return (B) getConstructorMap()
                .computeIfAbsent(
                        clazz,
                        key -> {
                            try {
                                Constructor<B> constructor = clazz.getDeclaredConstructor();
                                constructor.setAccessible(true);
                                return constructor;
                            } catch (Exception exception) {
                                throw new IllegalStateException(
                                        "Failed to find empty constructor for " + clazz);
                            }
                        })
                .newInstance();
    }

    public abstract <S> S executeRequest(ConnectionRequest<S> connectionRequest);

    public abstract <K> void start(Storage<K, T> holder);

    public abstract <K> void start(Storage<K, T> holder, boolean async);

    public abstract void shutdown();

    public abstract CompletableFuture<Boolean> confirmTable(DatabaseStructure modifiedStructure);

    public abstract <S extends Iterable<T>> CompletableFuture<List<T>> saveLoaded(S data, boolean async);

    public abstract <S extends Iterable<T>> CompletableFuture<List<T>> saveLoaded(S data, boolean async, boolean useRunner);

    public abstract CompletableFuture<List<T>> saveList(List<T> list, boolean async);

    public abstract CompletableFuture<List<T>> getKeyedList(String key, String keyValue, boolean async);

    public abstract CompletableFuture<List<SimpleStorageModel>> getSortedListByColumn(DatabaseStructure databaseStructure, String sortByColumnName, SQLDatabase.SortOrder sortOrder, int limit, int offset, boolean async);

    public abstract CompletableFuture<T> save(T object, boolean async);

    public abstract <K> CompletableFuture<T> load(Storage<K, T> holder, DataEntry<String, K> key, boolean async);

    public abstract <K> CompletableFuture<T> load(Storage<K, T> holder, DataEntry<String, K> key, boolean async, boolean persist);

    public abstract <K> CompletableFuture<List<T>> loadAll(Storage<K, T> holder, boolean async);

}
