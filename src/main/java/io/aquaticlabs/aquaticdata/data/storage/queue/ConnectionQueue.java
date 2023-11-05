package io.aquaticlabs.aquaticdata.data.storage.queue;

import io.aquaticlabs.aquaticdata.data.ADatabase;
import io.aquaticlabs.aquaticdata.util.DataDebugLog;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: extremesnow
 * On: 2/25/2023
 * At: 19:20
 */
public class ConnectionQueue {

    private final ADatabase aDatabase;
    private final List<ConnectionRequest<?>> queuedRequests;
    private boolean queueRunning;

    public ConnectionQueue(ADatabase aDatabase) {
        this.aDatabase = aDatabase;
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
            if (connectionRequest.getRunner() == null) {
                DataDebugLog.logDebug("Creating Connection Request with Null Runner.");
                aDatabase.executeNonLockConnection(connectionRequest);
            } else {
                connectionRequest.getRunner().accept(() -> aDatabase.executeNonLockConnection(connectionRequest));
            }
        } catch (Exception e) {
            e.printStackTrace();
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
