Java Data management made simple through OOP


How to use:

Create a holder class and an object that you want to be serialized. 



ObjectHolder.java
```java
public class ObjectHolder extends StorageHolder<UUID, SavableObject> {

    private final Map<UUID, SavableObject> dataMap = new ConcurrentHashMap<>();

    public ObjectHolder(DataCredential dataCredential) {
        super(dataCredential, UUID.class, SavableObject.class, StorageMode.LOAD_AND_STORE, Runnable::run, CompletableFuture::runAsync);
    }

    @Override
    public DatabaseStructure getStructure() {
        return new DatabaseStructure()
                .addColumn("uuid", SQLColumnType.VARCHAR_UUID)
                .addColumn("value", SQLColumnType.INT, 0)
                .addColumn("isEnabled", SQLColumnType.BOOLEAN, true);
    }

    @Override
    protected void onAdd(SavableObject object) {
        dataMap.put(object.getUuid(), object);
    }

    @Override
    protected void onRemove(SavableObject object) {
        dataMap.remove(object.getUuid());
    }

    @Override
    public SavableObject get(UUID key) {
        return dataMap.get(key);
    }

    @Override
    public Serializer<SavableObject> createSerializer() {

        return new ModelSerializer<SavableObject>().serializer((model, data) -> {
            data.write("uuid", model.getKey());
            data.write("value", model.getValue());
            data.write("isEnabled", model.isEnabled());
        }).deserializer((model, data) -> {
            if (model == null) {
                model = new SavableObject(data.applyAs("uuid", UUID.class));
            }
            model.setValue(data.applyAs("value", int.class));
            model.setEnabled(data.applyAs("isEnabled", boolean.class));
            return model;
        });
    }

    @Override
    public Iterator<SavableObject> iterator() {
        return dataMap.values().iterator();
    }
}

```


SavableObject.java
```java
@Getter @Setter
public class SavableObject implements StorageModel {

    private final UUID uuid;
    private int value;
    private boolean isEnabled;


    public SavableObject(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public Object getKey() {
        return uuid;
    }
}
```

