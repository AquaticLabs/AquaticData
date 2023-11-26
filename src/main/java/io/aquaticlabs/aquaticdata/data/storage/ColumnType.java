package io.aquaticlabs.aquaticdata.data.storage;

public enum ColumnType {
    INT("INT"),
    INTEGER("INTEGER"),
    FLOAT("FLOAT"),
    LONG("BIGINT"),
    TEXT("TEXT"),
    VARCHAR("VARCHAR(255)"),
    VARCHAR_UUID("VARCHAR(36)"),
    VARCHAR_UUID2("VARCHAR(37)"),
    VARCHAR_64("VARCHAR(64)"),
    DOUBLE("DOUBLE"),
    BOOLEAN("BOOLEAN"),
    TINY_INT("TINYINT"),
    BIT("BIT");

    private final String sql;

    ColumnType(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public static ColumnType matchType(String s) {

        for (ColumnType c : values()) {
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

    public static boolean isSimilarMatching(ColumnType type1, ColumnType type2) {
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