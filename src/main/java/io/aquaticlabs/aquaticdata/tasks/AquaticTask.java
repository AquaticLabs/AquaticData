package io.aquaticlabs.aquaticdata.tasks;

/**
 * @Author: extremesnow
 * On: 4/13/2023
 * At: 07:48
 */
public interface AquaticTask {

    int getId();
    String getOwner();

    void cancel();

}
