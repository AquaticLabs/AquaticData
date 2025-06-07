package testing.newtest;

import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.type.sql.sqlite.SQLiteCredential;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @Author: extremesnow
 * On: 11/3/2024
 * At: 17:40
 */
class NewTesting {

    LogDataHolder holder;

/*    @BeforeEach
    void setup() {
        File dataFile = new File("G:\\Projects\\DataStuff");
        holder = new LogDataHolder(new SQLiteCredential("minealerts_data", "minealerts_log_data", dataFile));
    }*/

/*
    @AfterEach
    void tearDown() throws IOException {
        holder.close();
    }
*/


/*    @Test
    void testBoot() {
        Assertions.assertNotNull(holder.getStructure());
    }
    */

    //@Test
    void testLoadKey() throws ExecutionException, InterruptedException, TimeoutException {
        System.out.println("LoadKEY");
        CompletableFuture<List<LogData>> dataFuture = holder.loadUUIDData(UUID.fromString("a749ef94-6d09-43b4-9abe-48cf93bfa961"));
        List<LogData> data = dataFuture.get(1, TimeUnit.MINUTES);

        System.out.println(data.size() + " loaded");
        System.out.println("AFTERKEY");

    }

    //@Test
    void testLoadDates() {
        System.out.println("LoadDates");

        List<String> dates = new ArrayList<>();

        holder.callSQLRequest(new ConnectionRequest<>((connection) -> {
            try (PreparedStatement statement = connection.prepareStatement("SELECT DISTINCT date(datetime(timestamp / 1000, 'unixepoch')) AS entry_date FROM minealerts_log_data ORDER BY entry_date")) {
                ResultSet set = statement.executeQuery();
                while (set.next()) {
                    String date = set.getString(1);
                    dates.add(date);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, Runnable::run));


        for (String date : dates) {
            System.out.println(date);
        }

    }

    private final Set<String> userNames = new HashSet<>();

    //@Test
    void testLoadNames() {
        System.out.println("LoadDates");

        holder.callSQLRequest(new ConnectionRequest<>((connection) -> {
            try (PreparedStatement statement = connection.prepareStatement("SELECT name FROM minealerts_log_data")) {
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    userNames.add(rs.getString(1));
                }
                System.out.println(userNames.size() + " usernames");

            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }, Runnable::run));

    }


   // @Test
    void testCheckData() throws ExecutionException, InterruptedException, TimeoutException {

        CompletableFuture<List<LogData>> dataFuture = holder.loadUUIDData(UUID.fromString("a749ef94-6d09-43b4-9abe-48cf93bfa961"));

        List<LogData> data = dataFuture.get(1, TimeUnit.MINUTES);

        LogData logData = data.get(0);
        CheckData checkData = logData.getCheckData();
/*        for (LogData data1 : data) {
            System.out.println(data1.getCheckData().toString());
        }*/
        System.out.println(checkData.toString());

        Assertions.assertFalse(data.get(0).getCheckData().isSurfaceOre());

    }



/*
    @Test
    void testObtainData() {
        Assertions.assertNotNull(holder.getOrNull(UUID.fromString("a749ef94-6d09-43b4-9abe-48cf93bfa961")));
        System.out.println(holder.get(233).getCheckType());

    }*/


}
