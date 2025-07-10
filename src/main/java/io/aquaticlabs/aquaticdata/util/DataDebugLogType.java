package io.aquaticlabs.aquaticdata.util;

import java.util.Set;

public enum DataDebugLogType {


    ALL_DEBUG(null),

    ALL_DATABASE(ALL_DEBUG),
    DATABASE_STARTUP(ALL_DATABASE),
    DATABASE_SHUTDOWN(ALL_DATABASE),

    ALL_SQL(ALL_DATABASE),
    SQL_SAVING(ALL_SQL),
    SQL_LOADING(ALL_SQL),
    SQL_INSERT(ALL_SQL),
    SQL_UPDATE(ALL_SQL),
    SQL_QUERIES(ALL_SQL),
    SQL_EXCEPTIONS(ALL_SQL),

    ALL_TASK_FACTORY(ALL_DEBUG),
    TASK_QUEUE(ALL_TASK_FACTORY),
    TASK_ADDED_TO_QUEUE(ALL_TASK_FACTORY),
    TASK_CLOSED_CONNECTION(ALL_TASK_FACTORY),
    TASK_CREATION(ALL_TASK_FACTORY),
    TASK_SHUTDOWN(ALL_TASK_FACTORY),

    OTHER(ALL_DEBUG);

    private final DataDebugLogType master;

    DataDebugLogType(DataDebugLogType master) {
        this.master = master;
    }

    public static boolean isDebug(Set<DataDebugLogType> list, DataDebugLogType dataDebugLogType) {
        return list.contains(ALL_DEBUG) || list.contains(dataDebugLogType) || dataDebugLogType.master == null || list.contains(dataDebugLogType.master);
    }
}
