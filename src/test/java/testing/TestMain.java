package testing;


import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.tasks.AquaticRunnable;
import io.aquaticlabs.aquaticdata.tasks.TaskFactory;
import io.aquaticlabs.aquaticdata.type.sql.SQLColumnData;
import io.aquaticlabs.aquaticdata.type.sql.SQLDatabase;
import io.aquaticlabs.aquaticdata.type.sql.sqlite.SQLiteCredential;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.DataDebugLogType;
import io.aquaticlabs.aquaticdata.util.DataEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: extremesnow
 * On: 8/24/2022
 * At: 21:16
 */
class TestMain {

    TestHolder holder;

    @BeforeEach
    void setup() {
        DataDebugLog.setDebug(true);
        DataDebugLog.setActiveLogTypes(DataDebugLogType.SQL_EXCEPTIONS);

        // holder = new TestHolder(new JsonCredential("TestingSB", "TestingTable", new File( "data.json")));
        holder = new TestHolder(new SQLiteCredential("TestingSB", "TestingTable", new File("")));
    }

    @AfterEach
    void tearDown() throws IOException {
        holder.close();
        //Files.deleteIfExists(Paths.get("TestingSB.db"));
    }

    @Test
    void testGetNull() {
        Assertions.assertNull(holder.getOrNull(UUID.randomUUID()));
    }

    void testGetUUID() {
        Assertions.assertNotNull(holder.loadIntoCache(UUID.fromString("f911d440-583e-4131-91ad-3e8b62dda1a1")));
    }

    //@Test
    void addEntry() {
        TestData data = new TestData(UUID.randomUUID());
        data.setName("Jeff");
        data.setValue(10);

        holder.create(data);

        TestData loadedData = holder.getOrCreate(data.getKey());

        Assertions.assertEquals(data.getKey(), loadedData.getKey());
        Assertions.assertEquals(data.getName(), loadedData.getName());
        Assertions.assertEquals(data.getValue(), loadedData.getValue());
    }

    //@Test
    void addBulk() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 0; i < 400000; i++) {
            TestData data = new TestData(UUID.randomUUID());
            data.setName("Mr Jeff: " + i);
            data.setValue(randomNumber(1, 100000));
            holder.add(data);
        }
        holder.saveAll(false);

    }

    @Test
    void testNewRank() {

        DatabaseStructure structure = new DatabaseStructure();
        structure.addColumn("uuid", new SQLColumnData<>(UUID.class));
        structure.addColumn("name", new SQLColumnData<>(String.class));
        structure.addColumn("value", new SQLColumnData<>(Integer.class));
        CompletableFuture<List<SimpleStorageModel>> future = holder.buildSortedStorageList(structure, "value", SQLDatabase.SortOrder.DESC, 25, true);

        try {
            List<SimpleStorageModel> list = future.get(10, TimeUnit.SECONDS);
            int i = 1;
            for (SimpleStorageModel model : list) {
                System.out.println("top #" + i + " name: " + model.getValue("name") + " val: " + model.getValue("value"));
                i++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //@Test
    void testRank() throws Exception {
        System.out.println("1");

        List<String> keyCols = new ArrayList<>();
        keyCols.add("uuid");
        keyCols.add("name");
        keyCols.add("value");

        Map<UUID, SimpleStorageModel> modelMap = holder.getStorageModelMap(keyCols, false).get(10, TimeUnit.SECONDS);

        Map<UUID, DataEntry<SimpleStorageModel, Object>> sortedMap = new LinkedHashMap<>();
        System.out.println(modelMap.size());

        Assertions.assertFalse(modelMap.isEmpty());
        int i = 0;
        for (Map.Entry<UUID, SimpleStorageModel> entry : modelMap.entrySet()) {
            sortedMap.put(entry.getKey(), new DataEntry<>(entry.getValue(), entry.getValue().getValue("value")));
            if (i <= 10) {
                System.out.println("model name: " + entry.getValue().getValue("name") + " val: " + entry.getValue().getValue("value"));
            }
            i++;
        }
        sortedMap = ObjectSorter.sortIntAndName(sortedMap);
        System.out.println("Sorted Map Size: " + sortedMap.size());

        i = 1;
        for (Map.Entry<UUID, DataEntry<SimpleStorageModel, Object>> entry : sortedMap.entrySet()) {
            System.out.println("top i " + i + " name: " + entry.getValue().getKey().getValue("name") + " val: " + entry.getValue().getValue());
            if (i >= 10) {
                break;
            }
            i++;
        }

        System.out.println("ranks");
    }

    public static Integer randomNumber(int min, int max) {
        Random i = new Random();
        if (max == min) {
            return max;
        } else {
            return min + i.nextInt(max - min);
        }
    }

    //@Test
    void dataExists() {
        TestData data = holder.get(UUID.fromString("838267c7-f097-4a2b-8289-94ec49b250ee"));
        System.out.println(holder.getDataMap().size());

        if (data != null) {
            System.out.println(data.toString());
        }
        Assertions.assertNotNull(data);

        Assertions.assertEquals(69, data.getValue());

    }


    void loadValueAndTimeOut() {

        TestData data = holder.loadIntoCache(UUID.fromString("1aba0a33-ee9d-4b4e-ad4f-a15236f3800e"));

        Assertions.assertEquals("Jeff", data.getName());

        holder.closeOut(data.getKey());

        Assertions.assertNull(holder.getOrNull(data.getKey()));

    }

    void loadValueAndTimeOut2() throws InterruptedException {

        TestData data = holder.loadIntoCache(UUID.fromString("c18762e4-a8f6-4e6a-8ed4-ea5e9e4f74ef"));

        Assertions.assertEquals("Jeff", data.getName());

        AtomicInteger secs = new AtomicInteger();

        TaskFactory factory = TaskFactory.getOrNew("Testing Factory");

        factory.createRepeatingTask(new AquaticRunnable() {
            @Override
            public void run() {
                System.out.println("Heart Beat: " + secs.getAndIncrement());
            }
        }, 1);

        while (secs.get() < 6) {
        }

        System.out.println("timoutTime: " + holder.getTimeOutTime());

        holder.cleanUp();
        Assertions.assertNull(holder.getOrNull(data.getKey()));

    }


}
