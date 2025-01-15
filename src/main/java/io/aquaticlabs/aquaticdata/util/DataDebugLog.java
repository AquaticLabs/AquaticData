package io.aquaticlabs.aquaticdata.util;


import io.aquaticlabs.aquaticdata.Database;
import lombok.Getter;
import lombok.Setter;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @Author: extremesnow
 * On: 3/18/2024
 * At: 18:51
 */

public class DataDebugLog {

    @Getter
    @Setter
    private static boolean debug = false;

    private static Logger logger;
    private static Logger publicLogger;

    static {
        logger = Logger.getLogger(DataDebugLog.class.getSimpleName());
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter() {
            @Override
            public String format(LogRecord record) {
                return String.format("[%s] %s: %s%n",
                        record.getLevel(),
                        record.getLoggerName(),
                        record.getMessage());
            }
        });
        logger.addHandler(handler);
        logger.setUseParentHandlers(false); // Disable default console logging

        publicLogger = Logger.getLogger(Database.class.getSimpleName());
        ConsoleHandler publicHandler = new ConsoleHandler();
        publicHandler.setFormatter(new SimpleFormatter() {
            @Override
            public String format(LogRecord record) {
                return String.format("[%s] %s: %s%n",
                        record.getLevel(),
                        record.getLoggerName(),
                        record.getMessage());
            }
        });
        publicLogger.addHandler(publicHandler);
        publicLogger.setUseParentHandlers(false); // Disable default console logging
    }


    public static void logDebug(Object debugMessage) {
        if (debug) {
            logger.log(Level.INFO, "Database Debug: " + debugMessage);
        }
    }

    public static void logConsole(Object debug) {
        publicLogger.log(Level.INFO, "" + debug);
    }

    public static void logError(Object debug) {
        logError(debug, null);
    }

    public static void logError(Object debug, Exception e) {
        logger.log(Level.WARNING, "Database Error: " + debug);
        if (e != null) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

}
