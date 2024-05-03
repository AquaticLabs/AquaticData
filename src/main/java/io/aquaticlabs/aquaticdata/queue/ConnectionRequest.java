package io.aquaticlabs.aquaticdata.queue;

import io.aquaticlabs.aquaticdata.data.HikariCPDatabase;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * @Author: extremesnow
 * On: 2/25/2023
 * At: 19:20
 */
@Getter
public class ConnectionRequest<T> {

    private final Executor executor;
    private final HikariCPDatabase.ConnectionCallback<T> request;
    private final List<Runnable> whenCompleteRunnables;

    public ConnectionRequest(HikariCPDatabase.ConnectionCallback<T> request, Executor executor) {
        this.request = request;
        this.executor = executor;
        this.whenCompleteRunnables = new ArrayList<>();
    }

    public ConnectionRequest<T> whenComplete(Runnable runnable) {
        whenCompleteRunnables.add(runnable);
        return this;
    }

}
