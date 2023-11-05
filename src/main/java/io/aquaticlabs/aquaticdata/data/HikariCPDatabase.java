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
            DataDebugLog.logDebug("Closed Connection.");
            for (Runnable runnable : connectionRequest.getWhenComplete()) {
                connectionRequest.getRunner().accept(runnable);
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