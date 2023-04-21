package io.aquaticlabs.aquaticdata.data.storage;

public enum ColumnType {
    INT("INT"),
    INTEGER("INTEGER"),
    FLOAT("FLOAT"),
    LONG("INT(8)"),
    TEXT("TEXT"),
    VARCHAR("VARCHAR(255)"),
    VARCHAR_UUID("VARCHAR(37)"),
    VARCHAR_64("VARCHAR(64)"),
    DOUBLE("DOUBLE"),
    BOOLEAN("BOOLEAN");

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
        return this.equals(VARCHAR) || this.equals(VARCHAR_UUID) || this.equals(VARCHAR_64) || this.equals(TEXT);
    }

    public boolean needsQuotes() {
        return this.equals(VARCHAR) || this.equals(VARCHAR_UUID) || this.equals(VARCHAR_64) || this.equals(TEXT);
    }

    public boolean isInt() {
        return this.equals(INT) || this.equals(INTEGER) || this.equals(LONG) || this.equals(BOOLEAN);
    }

    public static boolean isSimilarMatching(ColumnType type1, ColumnType type2) {
        if (type1.isVarchar() && type2.isVarchar()) {
            return true;
        }
        if (type1.isInt() && type2.isInt()) {
            return true;
        }
        return type1.getSql().equalsIgnoreCase(type2.getSql());
    }

}