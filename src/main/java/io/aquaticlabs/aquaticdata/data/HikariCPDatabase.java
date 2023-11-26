package io.aquaticlabs.aquaticdata.data;

import com.zaxxer.hikari.HikariDataSource;
import io.aquaticlabs.aquaticdata.data.storage.queue.ConnectionRequest;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class HikariCPDatabase extends ADatabase {

    @Setter(AccessLevel.PROTECTED)
    private HikariDataSource hikariDataSource;

    @SneakyThrows
    protected HikariCPDatabase(String dbTable) {
        super(dbTable);
    }

    public <T> T executeNonLockConnection(ConnectionRequest<T> connectionRequest) {

        try (Connection conn = hikariDataSource.getConnection()) {
            if (connectionRequest.getRunner() == null) {
                DataDebugLog.logDebug("executeNonLockCon: runner is null");
            }
            return connectionRequest.getRequest().doInConnection(conn);
        } catch (SQLException e) {
            throw new IllegalStateException("Error during SQL execution.", e);
        } finally {
            DataDebugLog.logDebug("Closed Connection. Runnables: " + connectionRequest.getWhenCompleteRunnables().size());
            for (Runnable runnable : connectionRequest.getWhenCompleteRunnables()) {
                DataDebugLog.logDebug("Running WhenCompleteRunnable");
                if (connectionRequest.getRunner() != null) {
                    connectionRequest.getRunner().accept(runnable);
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
        T doInConnection(Connection conn) throws SQLException;
    }
}