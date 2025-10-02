package testing;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: extremesnow
 * On: 9/30/2025
 * At: 16:50
 */
public enum SimpleStatType {

    VALUE("value", int.class, 0),
    DEATH("deaths", int.class, 0);


    private final String commonName;
    private final Object classType;
    private final Object defaultValue;

    SimpleStatType(String commonName, Object classType, Object defaultValue) {
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

    public static SimpleStatType matchStatType(String statType) {
        for (SimpleStatType type : SimpleStatType.values()) {
            if (type.name().equalsIgnoreCase(statType.toUpperCase()))
                return type;
        }
        return null;
    }

    public static List<String> commonNameValues() {
        List<String> list = new ArrayList<>();
        for (SimpleStatType value : values()) {
            list.add(value.commonName);
        }
        return list;
    }

    public static SimpleStatType matchCommonNameStat(String commonName) {
        for (SimpleStatType type : SimpleStatType.values()) {
            if (type.getCommonName().equalsIgnoreCase(commonName))
                return type;
        }
        return null;
    }

}
