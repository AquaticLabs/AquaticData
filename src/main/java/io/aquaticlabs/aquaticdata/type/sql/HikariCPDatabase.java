package io.aquaticlabs.aquaticdata.type.sql;

import com.zaxxer.hikari.HikariDataSource;
import io.aquaticlabs.aquaticdata.Database;
import io.aquaticlabs.aquaticdata.DatabaseStructure;
import io.aquaticlabs.aquaticdata.model.Serializer;
import io.aquaticlabs.aquaticdata.model.StorageModel;
import io.aquaticlabs.aquaticdata.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 18:43
 */
public abstract class HikariCPDatabase<T extends StorageModel> extends Database<T> {

    @Setter(AccessLevel.PROTECTED)
    private HikariDataSource hikariDataSource;

    protected HikariCPDatabase(DatabaseStructure tableStructure, Serializer<T> serializer, @NonNull Executor asyncExecutor, @NonNull Executor syncExecutor) {
        super(tableStructure, serializer, asyncExecutor, syncExecutor);
    }

    public <S> S executeRequest(ConnectionRequest<S> connectionRequest) {
        // Try with resources auto closes the connection.
        try (Connection connection = hikariDataSource.getConnection()) {
            if (connectionRequest.getExecutor() == null) {
                DataDebugLog.logDebug("executeRequest: runner is null");
            }
            return connectionRequest.getRequest().doInConnection(connection);
        } catch (SQLException e) {
            throw new IllegalStateException("Error during SQL execution.", e);
        } finally {
            DataDebugLog.logDebug("Closed Connection. Runnables: " + connectionRequest.getWhenCompleteRunnables().size());
            for (Runnable runnable : connectionRequest.getWhenCompleteRunnables()) {
                DataDebugLog.logDebug("Running WhenCompleteRunnable");
                if (connectionRequest.getExecutor() != null) {
                    connectionRequest.getExecutor().execute(runnable);
                } else {
                    runnable.run();
                }
            }
            getConnectionQueue().tryToExecuteNextInQueue();
        }
    }

    @Override
    public void shutdown() {
        if (hikariDataSource != null) hikariDataSource.close();
    }

    @SneakyThrows
    public void evict(Connection connection) {
        connection.close();
    }

    public interface ConnectionCallback<T> {
        T doInConnection(Connection connection) throws SQLException;
    }
}