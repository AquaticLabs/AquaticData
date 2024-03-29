package io.aquaticlabs.aquaticdata.data.storage.queue;

import io.aquaticlabs.aquaticdata.data.HikariCPDatabase;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * @Author: extremesnow
 * On: 2/25/2023
 * At: 19:20
 */
public class ConnectionRequest<T> {

    @Getter
    private final Consumer<Runnable> runner;
    @Getter
    private final HikariCPDatabase.ConnectionCallback<T> request;
    @Getter
    private final List<Runnable> whenCompleteRunnables;

    public ConnectionRequest(HikariCPDatabase.ConnectionCallback<T> request, Consumer<Runnable> runner) {
        this.request = request;
        this.runner = runner;
        this.whenCompleteRunnables = new ArrayList<>();
    }

    public ConnectionRequest<T> whenComplete(Runnable runnable) {
        whenCompleteRunnables.add(runnable);
        return this;
    }

}
