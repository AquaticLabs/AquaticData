package testing;


import io.aquaticlabs.aquaticdata.model.SimpleStorageModel;
import io.aquaticlabs.aquaticdata.tasks.TaskFactory;
import io.aquaticlabs.aquaticdata.type.sql.sqlite.SQLiteCredential;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
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

        holder = new TestHolder(new SQLiteCredential("TestingSB", "TestingTable", new File("")));
    }

    @AfterEach
    void tearDown() throws IOException {
        holder.close();
        //Files.deleteIfExists(Paths.get("TestingSB.db"));
    }

    void testGetNull() {
        Assertions.assertNull(holder.getOrNull(UUID.randomUUID()));
    }

    void testGetUUID() {
        Assertions.assertNotNull(holder.loadIntoCache(UUID.fromString("f911d440-583e-4131-91ad-3e8b62dda1a1")));
    }


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
        for (int i = 0; i < 10000; i++) {
            TestData data = new TestData(UUID.randomUUID());
            data.setName("Tony: " + i);
            data.setValue(randomNumber(1, 100000));
            holder.add(data);
        }
        holder.saveAll(false);

    }

   // @Test
    void testRank() throws Exception {
        System.out.println("1");

        holder.loadRanks("value");
        System.out.println("ranks");
    }


    void testGetSortedList() throws ExecutionException, InterruptedException, TimeoutException {

/*
        for (int i = 0; i < 100000; i++) {
            TestData data = new TestData(UUID.randomUUID());
            data.setName("Tony: " + i);
            data.setValue(randomNumber(1, 1341231));
            holder.add(data);
        }
        holder.saveAll(false);

*/

        try {
            List<SimpleStorageModel> sortedList = holder.getSortedDataList("value").get(1, TimeUnit.MINUTES);
            Assertions.assertFalse(sortedList.isEmpty());
            int firstVal = -1;
            for (SimpleStorageModel model : sortedList) {
                if (firstVal == -1) {
                    firstVal = (int) model.getValue("value");
                }
                System.out.println("Key: " + model.getKey() + " Name: " + model.getValue("name") + " Value: " + model.getValue("value"));
            }
            int lastVal = (int) sortedList.get(sortedList.size() - 1).getValue("value");

            Assertions.assertTrue(firstVal > lastVal);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static Integer randomNumber(int min, int max) {
        Random i = new Random();
        if (max == min) {
            return max;
        } else {
            return min + i.nextInt(max - min);
        }
    }

    void loadValueAndTimeOut() {

        TestData data = holder.loadIntoCache(UUID.fromString("c18762e4-a8f6-4e6a-8ed4-ea5e9e4f74ef"));

        Assertions.assertEquals("Jeff", data.getName());

        holder.closeOut(data.getKey());

        Assertions.assertNull(holder.getOrNull(data.getKey()));

    }

    void loadValueAndTimeOut2() throws InterruptedException {

        TestData data = holder.loadIntoCache(UUID.fromString("c18762e4-a8f6-4e6a-8ed4-ea5e9e4f74ef"));

        Assertions.assertEquals("Jeff", data.getName());

        AtomicInteger secs = new AtomicInteger();

        TaskFactory factory = TaskFactory.getOrNew("Testing Factory");
/*

        factory.createRepeatingTask(new AquaticRunnable() {
            @Override
            public void run() {
                System.out.println("Heart Beat: " + secs.getAndIncrement());
            }
        }, 1);

        while (secs.get() < 6) {
        }
*/

        System.out.println("timoutTime: " + holder.getTimeOutTime());

        holder.cleanUp();
        Assertions.assertNull(holder.getOrNull(data.getKey()));

    }


}
