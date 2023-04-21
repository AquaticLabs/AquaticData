package testing;

import java.util.ArrayList;
import java.util.List;

public enum StatType {

    KILL("kills", int.class, 0),
    DEATH("deaths", int.class, 0),
    KD("kd", double.class, 0.0),

    LEVEL("level", int.class, 1),
    EXPERIENCE("experience", int.class, 0),

    KILL_STREAK("kill_streak", int.class, 0),
    BEST_KILL_STREAK("best_kill_streak", int.class, 0);



    private final String commonName;
    private final Object classType;
    private final Object defaultValue;

    StatType(String commonName, Object classType, Object defaultValue) {
        this.commonName = commonName;
        this.classType = classType;
        this.defaultValue = defaultValue;
    }

    public String getCommonName() {
        return commonName;
    }

    public Object getClassType() {
        return classType;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public static StatType matchStatType(String statType) {
        for (StatType type : StatType.values()) {
            if (type.name().equalsIgnoreCase(statType.toUpperCase()))
                return type;
        }
        return null;
    }

    public static List<String> commonNameValues() {
        List<String> list = new ArrayList<>();
        for (StatType value : values()) {
            list.add(value.commonName);
        }
        return list;
    }

    public static StatType matchCommonNameStat(String commonName) {
        for (StatType type : StatType.values()) {
            if (type.getCommonName().equalsIgnoreCase(commonName))
                return type;
        }
        return null;
    }

}