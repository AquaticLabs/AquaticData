package io.aquaticlabs.aquaticdata.util;


import lombok.Getter;
import lombok.Setter;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 18:51
 */

public class DataDebugLog {

    @Getter
    @Setter
    private static boolean debug = false;

    public static void logDebug(Object debugMessage) {
        if (debug) {
            Logger logger = Logger.getLogger(DataDebugLog.class.getSimpleName());
            logger.log(Level.INFO, "Database Debug: " + debugMessage);
        }
    }

    public static void logError(Object debug) {
        logError(debug, null);
    }

    public static void logError(Object debug, Exception e) {
        Logger logger = Logger.getLogger(DataDebugLog.class.getSimpleName());
        logger.log(Level.WARNING, "Database Error: " + debug);
        if (e != null) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

}
