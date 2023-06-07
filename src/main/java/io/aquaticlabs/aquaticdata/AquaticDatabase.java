package io.aquaticlabs.aquaticdata;

import io.aquaticlabs.aquaticdata.data.tasks.TaskFactory;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * @Author: extremesnow
 * On: 8/22/2022
 * At: 19:10
 */
public class AquaticDatabase {

    @Getter
    private static AquaticDatabase instance;
    @Getter
    @Setter
    private Logger logger;
    @Getter
    private boolean debug = false;
    private Consumer<Runnable> asyncRunner;
    private Consumer<Runnable> syncRunner;

    public AquaticDatabase(@NonNull Consumer<Runnable> asyncRunner, @NonNull Consumer<Runnable> syncRunner, boolean debug, Logger logger) {
        if (instance != null) {
            System.out.println("Already an Instance of DataLib");
            return;
        }
        instance = this;
        this.logger = logger;
        this.debug = debug;
        this.asyncRunner = asyncRunner;
        this.syncRunner = syncRunner;
    }

    public Consumer<Runnable> getRunner(boolean async) {

        return async ? asyncRunner : syncRunner;
    }

    public static String getKey() {
        return "DS5qX8MZC0Sxqb9e";
    }

}
