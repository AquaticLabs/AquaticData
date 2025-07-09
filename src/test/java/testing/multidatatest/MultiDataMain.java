package testing.multidatatest;

import io.aquaticlabs.aquaticdata.type.sql.sqlite.SQLiteCredential;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.DataDebugLogType;
import org.junit.jupiter.api.*;
import testing.multidatatest.stat.StatModel;
import testing.multidatatest.stat.StatModelHolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MultiDataMain {

    static String DATABASE_FILE = "stat_models";
    static String TABLE_NAME = "placeholder_stat_";
    static Map<String, StatModelHolder> loadedStats = new ConcurrentHashMap<>();

    @BeforeAll
    static void setup() {
        DataDebugLog.setDebug(true);
        DataDebugLog.setActiveLogTypes(DataDebugLogType.SQL_EXCEPTIONS, DataDebugLogType.DATABASE_STARTUP, DataDebugLogType.SQL_LOADING);

        // Setup generic placeholder table:
        loadedStats.put("default", new StatModelHolder(new SQLiteCredential(DATABASE_FILE, TABLE_NAME + "default", new File(""))));


    }

    @AfterAll
    static void tearDown() throws IOException {
        for (StatModelHolder holder : loadedStats.values()) {
            holder.close();
        }
        loadedStats.clear();
    }

    @Test
    void testGetNullFromDefault() {
        StatModelHolder holder = loadedStats.get("default");
        Assertions.assertNotNull(holder);
        Assertions.assertNull(holder.get(UUID.randomUUID()));
    }
    @Test
    void addValueToDefault() {
        StatModelHolder holder = loadedStats.get("default");
        Assertions.assertNotNull(holder);
        StatModel model = new StatModel();
        model.setUuidKey(UUID.randomUUID());
        model.setName("Hailey");
        model.setValue(5);
        holder.insert(model);

        Assertions.assertNotNull(holder.loadOrInsert(model.getUuidKey()));
    }

    @Test
    void testAddNewPlaceholderHolder() {
        StatModelHolder holder = new StatModelHolder(new SQLiteCredential(DATABASE_FILE, TABLE_NAME + "firstTest", new File("")));
        Assertions.assertNotNull(holder);
        loadedStats.put("firstTest", holder);

        System.out.println(loadedStats.keySet());


    }


}
