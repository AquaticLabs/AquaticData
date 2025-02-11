package io.aquaticlabs.aquaticdata.tasks;

/**
 * @Author: extremesnow
 * On: 4/13/2023
 * At: 06:47
 */
public abstract class AquaticRunnable implements Runnable {

    private String ownerId = "";
    private int taskId = -1;

    public synchronized void cancel() throws IllegalStateException {
        TaskFactory.getFactory(getOwnerID()).cancelTask(getTaskId());
    }


    public void checkState() {
        if (taskId != -1) {
            throw new IllegalStateException("Already scheduled as " + taskId);
        }
    }

    public synchronized int getTaskId() throws IllegalStateException {
        final int id = taskId;
        if (id == -1) {
            throw new IllegalStateException("Not scheduled yet");
        }
        return id;
    }
    public synchronized String getOwnerID() throws IllegalStateException {
        final String owner = ownerId;
        if (owner.isEmpty()) {
            throw new IllegalStateException("Invalid owner, probably not scheduled yet");
        }
        return owner;
    }

    protected void setupTask(final AquaticTask task) {
        this.taskId = task.getId();
        this.ownerId = task.getOwner();
    }
}
