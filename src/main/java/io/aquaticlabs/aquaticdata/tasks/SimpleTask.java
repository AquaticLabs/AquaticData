package io.aquaticlabs.aquaticdata.tasks;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * @Author: extremesnow
 * On: 3/18/2023
 * At: 23:30
 */
public class SimpleTask implements AquaticTask {

    private final int id;
    private final String ownerId;
    private final Future<?> futureTask;
    private long startTime;
    private long endTime;
    private Consumer<Long> whenCompleteConsumer;

    public SimpleTask(TaskFactory owner, ScheduledExecutorService scheduledExecutorService, AquaticRunnable task) {
        this.id = owner.nextId();
        this.ownerId = owner.getOwnerID();
        task.setupTask(this);
        this.futureTask = scheduledExecutorService.submit(() -> {
            startTime = System.currentTimeMillis();
            task.run();
            endTime = System.currentTimeMillis();
            if (whenCompleteConsumer != null) {
                long executionTime = endTime - startTime;
                whenCompleteConsumer.accept(executionTime);
            }
        });
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getOwner() {
        return ownerId;
    }

    public void cancel() {
        futureTask.cancel(true);
    }

    public long getExecutionTimeMs() {
        if (startTime == 0 || endTime == 0) {
            throw new IllegalStateException("Task has not finished executing.");
        }
        return endTime - startTime;
    }

    public void whenComplete(Consumer<Long> consumer) {
        this.whenCompleteConsumer = consumer;
    }
}
