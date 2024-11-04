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
import java.util.List;
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
public class NewTesting {

    LogDataHolder holder;

    @BeforeEach
    void setup() {
        File dataFile = new File("G:\\Projects\\DataStuff");
        holder = new LogDataHolder(new SQLiteCredential("minealerts_data", "minealerts_log_data", dataFile));
    }

    @AfterEach
    void tearDown() throws IOException {
        holder.close();
    }


/*    @Test
    void testBoot() {
        Assertions.assertNotNull(holder.getStructure());
    }
    */

    @Test
    void testLoadKey() throws ExecutionException, InterruptedException, TimeoutException {
        System.out.println("LoadKEY");
        CompletableFuture<List<LogData>> dataFuture = holder.loadUUIDData(UUID.fromString("a749ef94-6d09-43b4-9abe-48cf93bfa961"));
        List<LogData> data = dataFuture.get(1, TimeUnit.MINUTES);

        System.out.println(data.size() + " loaded");
        System.out.println("AFTERKEY");

    }

    @Test
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




/*
    @Test
    void testObtainData() {
        Assertions.assertNotNull(holder.getOrNull(UUID.fromString("a749ef94-6d09-43b4-9abe-48cf93bfa961")));
        System.out.println(holder.get(233).getCheckType());

    }*/


}
