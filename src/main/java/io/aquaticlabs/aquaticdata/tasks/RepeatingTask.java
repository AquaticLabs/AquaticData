package io.aquaticlabs.aquaticdata.tasks;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Author: extremesnow
 * On: 3/18/2023
 * At: 23:30
 */
public class RepeatingTask implements AquaticTask {

    private final int id;
    private final String ownerId;
    private final ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture<?> futureTask;
    private Runnable myTask;
    private long interval;
    private final TimeUnit timeUnit;

    public RepeatingTask(TaskFactory owner, ScheduledExecutorService scheduledExecutorService, AquaticRunnable task, long interval, long delay, TimeUnit timeUnit) {
        this.id = owner.nextId();
        this.ownerId = owner.getOwnerID();
        task.setupTask(this);
        this.scheduledExecutorService = scheduledExecutorService;
        this.timeUnit = timeUnit;
        this.interval = interval;
        this.futureTask = scheduledExecutorService.scheduleAtFixedRate(task, delay, interval, timeUnit);
        this.myTask = task;
    }

    public synchronized void setOrResetInterval(long time) {
        if (time > 0) {
            futureTask.cancel(true);
            this.interval = time;
            futureTask = scheduledExecutorService.scheduleAtFixedRate(myTask, 0, time, timeUnit);
        }
    }

    public synchronized void resetTask(Runnable task) {
        futureTask.cancel(true);
        this.myTask = task;
        futureTask = scheduledExecutorService.scheduleAtFixedRate(myTask, 0, interval, timeUnit);
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
