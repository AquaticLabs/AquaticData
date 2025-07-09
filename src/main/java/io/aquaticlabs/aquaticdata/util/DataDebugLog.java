package io.aquaticlabs.aquaticdata.util;


import io.aquaticlabs.aquaticdata.Database;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    private static final Logger logger;
    private static final Logger publicLogger;

    private static final Set<DataDebugLogType> activeLogTypes = new HashSet<>();

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
        Arrays.stream(logger.getHandlers()).sequential().forEach(logger::removeHandler);
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
        Arrays.stream(publicLogger.getHandlers()).sequential().forEach(publicLogger::removeHandler);
        publicLogger.addHandler(publicHandler);
        publicLogger.setUseParentHandlers(false); // Disable default console logging
    }


    public static void logDebug(DataDebugLogType logType, Object debugMessage) {
        if (debug && activeLogTypes.contains(logType)) {
            logger.log(Level.INFO, "Log Type: " + logType.name() + " : " + debugMessage);
        }
    }

    public static void setActiveLogTypes(DataDebugLogType... types) {
        activeLogTypes.clear();
        activeLogTypes.addAll(List.of(types));
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
