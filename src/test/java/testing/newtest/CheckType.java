package testing.newtest;

import lombok.Getter;

/**
 * @Author: extremesnow
 * On: 5/5/2023
 * At: 17:11
 */
@Getter
public enum CheckType {

    SINGLE("Single", true, 0, 0, 0, 0),
    VEIN("Vein", true, 0, 0, 0, 0),
    TIMED_SINGLE( "Timed-Single", true, 3, 20, 5, 0),
    TIMED_VEIN("Timed-Vein", true, 3, 10, 5, 0),

    DISTANCE( "Distance", true, 5, 0, 0, 10),
    LIGHT_LEVEL( "Light-Level", true, 5, 5, 0, 0),
    RATIO("Ratio", true, 0, 0, 0, 0),
    SUS( "Sus-Level", false, 0, 0, 0, 0),

    CUSTOM(null, false, 0, 0, 0, 0);


    private final String fileCommonName;
    private final boolean blockRelated;
    private final int defaultSeverity;
    private final int defaultThreshold;
    private final int defaultTimeAmount;
    private final int defaultBlockWindow;

    CheckType(String fileCommonName, boolean blockRelated, int defaultSeverity, int defaultThreshold, int defaultTimeAmount, int defaultBlockWindow) {
        this.fileCommonName = fileCommonName;
        this.blockRelated = blockRelated;
        this.defaultSeverity = defaultSeverity;
        this.defaultThreshold = defaultThreshold;
        this.defaultTimeAmount = defaultTimeAmount;
        this.defaultBlockWindow = defaultBlockWindow;
    }

    public static CheckType matchType(String typeName) {
        for (CheckType checkType : values()) {
            if (checkType.name().equalsIgnoreCase(typeName)) {
                return checkType;
            }
        }
        return null;
    }
}
