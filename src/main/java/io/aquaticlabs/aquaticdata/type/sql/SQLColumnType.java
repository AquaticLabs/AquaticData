package io.aquaticlabs.aquaticdata.type.sql;

import lombok.Getter;

@Getter
public enum SQLColumnType {
    INT("INT", int.class),
    INTEGER("INTEGER", int.class),
    FLOAT("FLOAT", float.class),
    LONG("BIGINT", long.class),
    TEXT("TEXT", String.class),
    VARCHAR("VARCHAR(255)", String.class),
    VARCHAR_UUID("VARCHAR(36)", String.class),
    VARCHAR_UUID2("VARCHAR(37)", String.class),
    VARCHAR_64("VARCHAR(64)", String.class),
    DOUBLE("DOUBLE", double.class),
    BOOLEAN("BOOLEAN", boolean.class),
    TINY_INT("TINYINT", int.class),
    BIT("BIT", int.class);

    private final String sql;
    private final Class<?> associatedClass;

    SQLColumnType(String sql, Class<?> associatedClass) {
        this.sql = sql;
        this.associatedClass = associatedClass;
    }

    public static SQLColumnType matchType(String s) {

        for (SQLColumnType c : values()) {
            if (c.getSql().equalsIgnoreCase(s)) {
                return c;
            }
            if (c.getSql().split("\\(")[0].equalsIgnoreCase(s)) {
                return c;
            }
        }
        return null;
    }

    public boolean isVarchar() {
        return this.equals(VARCHAR) || this.equals(VARCHAR_UUID) || this.equals(VARCHAR_UUID2) || this.equals(VARCHAR_64) || this.equals(TEXT);
    }
    public boolean isBoolean() {
        return this.equals(BOOLEAN) || this.equals(BIT) || this.equals(TINY_INT);
    }

    public boolean needsQuotes() {
        return isVarchar();
    }

    public boolean isInt() {
        return this.equals(INT) || this.equals(INTEGER);
    }

    public static SQLColumnType matchColumnClassType(Class<?> clazz) {
        for (SQLColumnType type : values()) {
            if (type.getAssociatedClass().equals(clazz)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported class type: " + clazz.getName());
    }

    public static boolean isSimilarMatching(SQLColumnType type1, SQLColumnType type2) {
        if (type1.isVarchar() && type2.isVarchar()) {
            return true;
        }
        if (type1.isInt() && type2.isInt()) {
            return true;
        }
        if (type1.isBoolean() && type2.isBoolean()) {
            return true;
        }
        return type1.getSql().equalsIgnoreCase(type2.getSql());
    }

}