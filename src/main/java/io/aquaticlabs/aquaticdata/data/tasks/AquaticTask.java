package io.aquaticlabs.aquaticdata.data.tasks;

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
