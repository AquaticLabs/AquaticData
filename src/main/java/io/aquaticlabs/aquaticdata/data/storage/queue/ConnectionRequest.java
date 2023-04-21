package io.aquaticlabs.aquaticdata.data.storage.queue;

import io.aquaticlabs.aquaticdata.data.HikariCPDatabase;
import lombok.Getter;

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
    private final HikariCPDatabase.ConnectionCallback<T> callback;

    public ConnectionRequest(HikariCPDatabase.ConnectionCallback<T> callback, Consumer<Runnable> runner) {
        this.callback = callback;
        this.runner = runner;
    }

}
