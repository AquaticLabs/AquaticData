package testing;

import io.aquaticlabs.aquaticdata.AquaticDatabase;
import io.aquaticlabs.aquaticdata.data.object.DataEntry;
import io.aquaticlabs.aquaticdata.data.storage.ColumnType;
import io.aquaticlabs.aquaticdata.data.storage.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.tasks.AquaticRunnable;
import io.aquaticlabs.aquaticdata.tasks.RepeatingTask;
import io.aquaticlabs.aquaticdata.tasks.TaskFactory;
import io.aquaticlabs.aquaticdata.data.type.DataCredential;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * @Author: extremesnow
 * On: 8/24/2022
 * At: 21:16
 */
public class TestMain {

    public static TestHold testHold;

    public static void main(String[] args) {
        System.out.println(new File("resources").toPath());

        AquaticDatabase aquaticDatabase = new AquaticDatabase(CompletableFuture::runAsync, Runnable::run, true, Logger.getLogger("AqLabs"));

        DataCredential sqlite = new DataCredential().SQLiteCredential(new File("G:\\Projects\\DataStuff"), "testDataConfirmTables", "testDataConfirmTables");
        //DataCredential mariaDB = new DataCredential().MariaDBCredential("testtable", "localhost", 3306, "root", "password", "testingMariaDB");

        testHold = new TestHold(sqlite);


        System.out.println(buildCreateTableSQL(testHold.getStructure(),false));

        TestData testData = testHold.getOrInsert(new TestData(UUID.fromString("269d4132-8758-458f-9087-344325ee14cf"), "Rod", 42));

        //testHold.saveSingle(testData,true);
/*        TestData testData2 = testHold.getOrInsert(new TestData(UUID.randomUUID(), "Shod", 11));
        TestData testData3 = testHold.getOrInsert(new TestData(UUID.randomUUID(), "Brodd", 24));
        TestData testData4 = testHold.getOrInsert(new TestData(UUID.randomUUID(), "Nod", 5));
        TestData testData5 = testHold.getOrInsert(new TestData(UUID.randomUUID(), "Lodd", 53));*/


        testHold.saveLoaded(false);


        AtomicInteger secs = new AtomicInteger();

        TaskFactory factory = TaskFactory.getOrNew("Testing Factory");
        RepeatingTask heart = factory.createRepeatingTask(new AquaticRunnable() {
            @Override
            public void run() {
                System.out.println("Heart Beat: " + secs.getAndIncrement());

            }
        }, 1);


        factory.createDelayedTask(new AquaticRunnable() {
            @Override
            public void run() {
                DataDebugLog.logDebug("DELAYED TASK RAN");
                TestData data = testHold.getOrNull(UUID.fromString("269d4132-8758-458f-9087-344325ee14cf"));
                DataDebugLog.logDebug("UUID: " + data.uuid + " Name: " + data.name + " val: " + data.getLevel());

            }
        }, 3);


/*
        factory.createDelayedTask(new AquaticRunnable() {
            @Override
            public void run() {
                int i = 0;
                for (TestData data : testHold.getData().values()) {
                    System.out.println(data.toString());
                    i++;
                    if (i > 3) {
                        break;
                    }
                }
                System.out.println("Current Task ID: " + getTaskId());
                System.out.println("Current TaskFactory Owner: " + getOwnerID());


                TestData data = testHold.getOrNull(UUID.fromString("90b0aac2-2e16-48ea-a711-2066593386ba"));
                System.out.println("Data OBJ2: ");
                System.out.println(data.toString());
            }

        }, 5);

*/

        System.out.println("cache time seconds to save: " + testHold.getCacheTimeInSecondsToSave());

        testHold.getDatabase().getConnectionQueue().addConnectionRequest(new ConnectionRequest<>(conn -> {
            System.out.println("DataSize: " + testHold.getDataSize(conn));
            return null;
        }, aquaticDatabase.getRunner(true)));


        boolean quit = false;
        while (secs.get() < 15) {
        }
        factory.shutdown();
        //AquaticDatabase.getInstance().shutdown();
        testHold.shutdown();

    }
    private static String buildCreateTableSQL(List<DataEntry<String, ColumnType>> columns, boolean force) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE ");
        if (!force) {
            queryBuilder.append("IF NOT EXISTS ");
        }
        queryBuilder
                .append("testtable")
                .append(" (");

        for (int i = 0; i < columns.size(); i++) {
            DataEntry<String, ColumnType> column = columns.get(i);
            queryBuilder.append(column.getKey()).append(" ").append(column.getValue().getSql()); // maybe add not null?
            if (i != columns.size() - 1) {
                queryBuilder.append(", ");
            }
        }
        queryBuilder
                .append(", PRIMARY KEY ( ")
                .append(columns.get(0).getKey())
                .append(" ));");

        DataDebugLog.logDebug("MYSQL TABLE CREATION: " + queryBuilder);

        return queryBuilder.toString();
    }
}
