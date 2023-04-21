package io.aquaticlabs.aquaticdata.util;

import io.aquaticlabs.aquaticdata.AquaticDatabase;

import java.util.logging.Level;

/**
 * @Author: extremesnow
 * On: 12/27/2021
 * At: 16:09
 */
public class DataDebugLog {

    public static void logDebug(Object debug) {
        if (AquaticDatabase.getInstance().isDebug()) {
            AquaticDatabase.getInstance().getLogger().log(Level.INFO, "Aquatic Data Debug: " + debug);
        }
    }

    public static void logError(Object debug) {
        AquaticDatabase.getInstance().getLogger().log(Level.WARNING, "Aquatic Data Error: " + debug);

    }

}
