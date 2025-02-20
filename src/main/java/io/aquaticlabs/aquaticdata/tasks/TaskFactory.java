package io.aquaticlabs.aquaticdata.tasks;

import io.aquaticlabs.aquaticdata.util.DataDebugLog;
import io.aquaticlabs.aquaticdata.util.FactoryExistsThrowable;
import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: extremesnow
 * On: 3/19/2023
 * At: 19:43
 */
public class TaskFactory {

    private static final Set<TaskFactory> factories = new HashSet<>();
    @Getter
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    public static TaskFactory getFactory(String ownerID) {
        TaskFactory factory = null;
        for (TaskFactory fac : factories) {
            if (ownerID.equalsIgnoreCase(fac.getOwnerID())) {
                factory = fac;
                break;
            }
        }
        return factory;
    }


    @Getter
    private final String ownerID;

    @Getter
    private int currentID = 1;

    private final Map<Integer, AquaticTask> activeTasks = new HashMap<>();

    @Getter
    private final ScheduledExecutorService scheduledExecutorService;

    private TaskFactory(String ownerID) {
        this.ownerID = ownerID;
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        factories.add(this);
    }

    public static TaskFactory getOrNew(String ownerID) {
        if (factories.stream().anyMatch(taskFactory -> taskFactory.getOwnerID().equalsIgnoreCase(ownerID))) {
            return getFactory(ownerID);
        }
        return new TaskFactory(ownerID);
    }

    public static TaskFactory create(String ownerID) throws FactoryExistsThrowable {
        if (factories.stream().anyMatch(taskFactory -> taskFactory.getOwnerID().equalsIgnoreCase(ownerID))) {
            throw new FactoryExistsThrowable("Factory " + ownerID + " already exists.");
        }
        return new TaskFactory(ownerID);
    }



    public SimpleTask runTask(AquaticRunnable runnable) {
        if (isShuttingDown.get()) {
            DataDebugLog.logDebug("Task creation ignored as Task Factory is shutting down: " + ownerID);
            return null; // or an appropriate no-op task if needed
        }
        SimpleTask task = new SimpleTask(this, scheduledExecutorService, runnable);
        activeTasks.put(task.getId(), task);
        return task;
    }

    /**
     * Creates a new instance of {@link RepeatingTask} with the specified runnable and interval.
     *
     * @param runnable the runnable to execute repeatedly
     * @param interval the time in seconds between each execution of the runnable
     * @return a new instance of {@link RepeatingTask}
     */
    public RepeatingTask createRepeatingTask(AquaticRunnable runnable, long interval) {
        return createRepeatingTask(runnable, interval, 0, TimeUnit.SECONDS);
    }

    /**
     * Creates a new instance of {@link RepeatingTask} with the specified task and interval.
     *
     * @param runnable the task to execute repeatedly
     * @param interval the time in seconds between each execution of the task
     * @param delay    the time in seconds before first execution of the task
     * @return a new instance of {@link RepeatingTask}
     */
    public RepeatingTask createRepeatingTask(AquaticRunnable runnable, long interval, long delay) {
        return createRepeatingTask(runnable, interval, delay, TimeUnit.SECONDS);
    }

    /**
     * Creates a new instance of {@link RepeatingTask} with the specified task and interval.
     *
     * @param runnable the task to execute repeatedly
     * @param interval the time in timeUnit between each execution of the task
     * @param delay    the time in timeUnit before first execution of the task
     * @param timeUnit the timeUnit used for the task
     * @return a new instance of {@link RepeatingTask}
     */
    public RepeatingTask createRepeatingTask(AquaticRunnable runnable, long interval, long delay, TimeUnit timeUnit) {
        if (isShuttingDown.get()) {
            DataDebugLog.logDebug("Task creation ignored as Task Factory is shutting down: " + ownerID);
            return null; // or an appropriate no-op task if needed
        }
        RepeatingTask task = new RepeatingTask(this, scheduledExecutorService, runnable, interval, delay, timeUnit);
        activeTasks.put(task.getId(), task);
        return task;
    }

    /**
     * Creates a new instance of {@link DelayedTask} with the specified task and interval.
     *
     * @param runnable the task to delay execution
     * @param delay    the time in seconds till execution
     * @return a new instance of {@link DelayedTask}
     */
    public DelayedTask createDelayedTask(AquaticRunnable runnable, long delay) {
        if (isShuttingDown.get()) {
            DataDebugLog.logDebug("Task creation ignored as Task Factory is shutting down: " + ownerID);
            return null; // or an appropriate no-op task if needed
        }
        DelayedTask task = new DelayedTask(this, scheduledExecutorService, runnable, delay);
        activeTasks.put(task.getId(), task);
        return task;
    }

    /**
     * Shuts down the {@link ScheduledExecutorService} used by this factory.
     * Any scheduled tasks will be cancelled and any active tasks will be allowed to complete.
     */
    public void shutdown() {
        DataDebugLog.logDebug("Shutting down Task Factory " + ownerID);

        // Initiate shutdown
        isShuttingDown.set(true);
        scheduledExecutorService.shutdown();
        DataDebugLog.logConsole("Database may take up to 60 seconds to shutdown.");

        try {
            // Wait for existing tasks to complete
            if (!scheduledExecutorService.awaitTermination(60, TimeUnit.SECONDS)) {
                DataDebugLog.logDebug("Tasks did not terminate in the specified timeout. Forcing shutdown...");
                List<Runnable> canceledTasks = scheduledExecutorService.shutdownNow();
                DataDebugLog.logDebug(canceledTasks.size() + " tasks were forcefully stopped.");
            } else {
                DataDebugLog.logDebug("Task Factory " + ownerID + " shut down gracefully.");
            }
        } catch (InterruptedException e) {
            DataDebugLog.logDebug("Shutdown interrupted. Forcing shutdown...");
            List<Runnable> canceledTasks = scheduledExecutorService.shutdownNow();
            DataDebugLog.logDebug(canceledTasks.size() + " tasks were forcefully stopped.");
            Thread.currentThread().interrupt();
        }
    }


    public int nextId() {
        return currentID++;
    }

    public synchronized void cancelTask(int taskId) {
        AquaticTask task = activeTasks.remove(taskId);
        if (task == null) {
            DataDebugLog.logError("Tried to cancel a task that wasn't scheduled.");
            return;
        }
        task.cancel();
    }
}
