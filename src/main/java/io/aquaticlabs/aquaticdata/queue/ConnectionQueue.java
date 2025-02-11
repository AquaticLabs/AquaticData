package io.aquaticlabs.aquaticdata.queue;

import io.aquaticlabs.aquaticdata.Database;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: extremesnow
 * On: 2/25/2023
 * At: 19:20
 */
public class ConnectionQueue {

    private final Database<?> database;
    private final List<ConnectionRequest<?>> queuedRequests;
    @Getter
    private boolean queueRunning;

    public ConnectionQueue(Database<?> database) {
        this.database = database;
        this.queuedRequests = new ArrayList<>();
    }

    public synchronized void tryToExecuteNextInQueue() {
        if (queuedRequests.isEmpty()) {
            DataDebugLog.logDebug("Queue Empty.");
            queueRunning = false;
            return;
        }
        queueRunning = true;
        DataDebugLog.logDebug("Executing Next in Queue. Queue Size: " + queuedRequests.size());
        ConnectionRequest<?> connectionRequest = queuedRequests.remove(0);
        try {
            if (connectionRequest.getExecutor() == null) {
                DataDebugLog.logDebug("Creating Connection Request with Null Executor.");
                database.executeRequest(connectionRequest);
            } else {
                connectionRequest.getExecutor().execute(() -> database.executeRequest(connectionRequest));
            }
        } catch (Exception e) {
            DataDebugLog.logError("Connection Request Exception: ", e);
        }
    }

    public void addConnectionRequest(ConnectionRequest<?> request) {
        DataDebugLog.logDebug("Added new Connection Request");
        queuedRequests.add(request);
        if (!queueRunning) {
            tryToExecuteNextInQueue();
        }
    }
}
