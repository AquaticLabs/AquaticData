package testing.multidatatest;

import io.aquaticlabs.aquaticdata.type.sql.sqlite.SQLiteCredential;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.DataDebugLogType;
import org.junit.jupiter.api.*;
import testing.multidatatest.stat.StatModel;
import testing.multidatatest.stat.StatModelHolder;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MultiDataMain {

    static String DATABASE_FILE = "stat_models";
    static String TABLE_NAME = "placeholder_stat_";
    static Map<String, StatModelHolder> loadedStats = new ConcurrentHashMap<>();

    @BeforeAll
    static void setup() {
        DataDebugLog.setDebug(true);
        DataDebugLog.setActiveLogTypes(DataDebugLogType.SQL_EXCEPTIONS, DataDebugLogType.DATABASE_STARTUP, DataDebugLogType.SQL_LOADING, DataDebugLogType.TASK_SHUTDOWN, DataDebugLogType.TASK_ADDED_TO_QUEUE);

        // Setup generic placeholder table:
        loadedStats.put("default", new StatModelHolder(new SQLiteCredential(DATABASE_FILE, TABLE_NAME + "default", new File(""))));
        loadedStats.put("firstTest", new StatModelHolder(new SQLiteCredential(DATABASE_FILE, TABLE_NAME + "firstTest", new File(""))));


    }

    @AfterAll
    static void tearDown() {
        for (StatModelHolder holder : loadedStats.values()) {
            holder.close();
        }
        loadedStats.clear();
    }

    @Test
    void addBulk() throws ExecutionException, InterruptedException, TimeoutException {
        StatModelHolder holder = loadedStats.get("default");

        for (int i = 0; i < 1000; i++) {
            StatModel data = new StatModel(UUID.randomUUID());
            data.setName("Mr Jeff: " + i);
            data.setValue(randomNumber(1, 100000));
            holder.insert(data);
        }
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
        StatModel model = new StatModel(UUID.randomUUID());
        model.setName("Hailey");
        model.setValue(5);
        holder.insert(model);
        StatModel loadedModel = holder.loadOrInsert(model.getUuidKey());
        Assertions.assertNotNull(loadedModel);
    }

    //@Test
    void testAddNewPlaceholderHolder() {
        StatModelHolder holder = new StatModelHolder(new SQLiteCredential(DATABASE_FILE, TABLE_NAME + "firstTest", new File("")));
        Assertions.assertNotNull(holder);
        loadedStats.put("firstTest", holder);

        System.out.println(loadedStats.keySet());
    }

    @Test
    void addValueToTester() {
        StatModelHolder holder = loadedStats.get("firstTest");
        Assertions.assertNotNull(holder);
        StatModel model = new StatModel(UUID.randomUUID());
        model.setName("Hailey");
        model.setValue(5);
        holder.insert(model);

        StatModel loadedModel = holder.loadOrInsert(model.getUuidKey());
        Assertions.assertNotNull(loadedModel);
    }

    public static Integer randomNumber(int min, int max) {
        Random i = new Random();
        if (max == min) {
            return max;
        } else {
            return min + i.nextInt(max - min);
        }
    }
}
