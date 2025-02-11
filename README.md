# AquaticData

AquaticData is a lightweight, object-oriented data management library designed for Java developers. It simplifies database interaction while maintaining flexibility and scalability. Currently, it supports **SQLite**, **MySQL**, and **MariaDB** databases, with plans to include support for **flatfile** and **MongoDB** in future updates.

---

## Features
- **Object-Oriented Data Management**: Utilize Java's OOP principles for streamlined database operations.
- **Multi-Database Support**: Seamlessly integrate with SQLite, MySQL, and MariaDB.
- **Extensible Design**: Future updates will bring support for flatfile storage and MongoDB.
- **Ease of Use**: Simplify database interactions with intuitive APIs.

---

## Installation

### Maven
Add the following to your `pom.xml`:
```xml
<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
	</repositories>

	<dependency>
	    <groupId>com.github.AquaticLabs</groupId>
	    <artifactId>AquaticData</artifactId>
	    <version>2.1</version>
	</dependency>
```

### Gradle
Add the following to your `build.gradle`:
```gradle
	dependencyResolutionManagement {
		repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
		repositories {
			mavenCentral()
			maven { url 'https://jitpack.io' }
		}
	}

	dependencies {
	        implementation 'com.github.AquaticLabs:AquaticData:2.1'
	}
```

---

## Usage

### Basic Setup
1. **Initialize the Library**:
   
    You will need to initialize the database and create a 'DataCredential' this credential is what the library uses to login to the 
    database and create your table.  

    Example credential setup.
```java
   //SQLite                                                                   // creates a file if one is not present.
    Credential credential = new SQLiteCredential("DatabaseName", "TableName", new File("FileLocation"))
   //MySQL
    Credential credential = new MySQLCredential("DatabaseName", "Hostname", port, "username", "password", "TableName")
   //MariaDB
    Credential credential = new MariaDBCredential("DatabaseName", "Hostname", port, "username", "password", "TableName")
```

You'll also want to create a holder class object that can manage all your data operations. An example of this class is defined below:

```java

  ObjectHolder objectHolder = new ObjectHolder(credential);

```

2. **Define Data Models**:
```java
   // Creating your data holder class:
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

```java
// Simple data class
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


3. **Perform Operations**:
   ```java
   // You'll want to setup some getters inside your holder to retrieve data from either your dataMap or if you dont store the data, you'll want to fetch the data by running load(key)

   SavableObject savableObject = holder.getObject(uuid);
   
   holder.save(savableObject, boolean async);
   holder.saveLoaded(boolean async);
   holder.saveList(List<SavableObject> list, boolean async);
   holder.load(key);
   holder.loadAll();
   holder.getSortedListByColumn(DatabaseStructure databaseStructure, String sortByColumnName, SQLDatabase.SortOrder sortOrder, int limit, int offset, boolean async)
   
   // you can also run manual query's to the database by running:
   //immediate
   holder.executeRequest(new ConnectionRequest((connection) -> {
      try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
          statement.executeUpdate();
      }
   }));
   // add to a execution queue
   holder.addExecuteRequest(new ConnectionRequest((connection) -> {
      try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
          statement.executeUpdate();
      }
   }));
   ```
4. **Shutdown**:

```java
    // to shutdown the database all you have to do is call
    holder.shutdown();


```


---

## Roadmap
- **Flatfile Support**: Enable storing data in YAML or JSON formats.
- **MongoDB Support**: Add compatibility for NoSQL databases.
- **Enhanced Query Builder**: Simplify complex query creation.

---

this stuff is filler for now...
 \/

## Contributing
We welcome contributions to improve AquaticData! Check out our [Contribution Guidelines](https://github.com/AquaticLabs/AquaticData/blob/2.1-update/CONTRIBUTING.md) to get started.

---

## License
This project is licensed under the [MIT License](https://github.com/AquaticLabs/AquaticData/blob/2.1-update/LICENSE).

---

## Links
- **Documentation**: [AquaticData Docs](https://github.com/AquaticLabs/AquaticData/tree/2.1-update)
- **Issues**: [Report Issues](https://github.com/AquaticLabs/AquaticData/issues)
- **Releases**: [Changelog](https://github.com/AquaticLabs/AquaticData/releases)

---

Thank you for using AquaticData! 🚀

