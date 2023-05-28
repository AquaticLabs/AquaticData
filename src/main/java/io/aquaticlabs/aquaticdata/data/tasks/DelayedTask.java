package io.aquaticlabs.aquaticdata.data.tasks;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Author: extremesnow
 * On: 3/18/2023
 * At: 23:30
 */
public class DelayedTask implements AquaticTask {

    private final int id;
    private final String ownerId;
    private final ScheduledFuture<?> futureTask;

    public DelayedTask(TaskFactory owner, ScheduledExecutorService scheduledExecutorService, AquaticRunnable task, long delay) {
        this.id = owner.nextId();
        this.ownerId = owner.getOwnerID();
        task.setupTask(this);
        futureTask = scheduledExecutorService.schedule(task, delay, TimeUnit.SECONDS);
    }
    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getOwner() {
        return ownerId;
    }

    @Override
    public void cancel() {
        futureTask.cancel(true);
    }
}
