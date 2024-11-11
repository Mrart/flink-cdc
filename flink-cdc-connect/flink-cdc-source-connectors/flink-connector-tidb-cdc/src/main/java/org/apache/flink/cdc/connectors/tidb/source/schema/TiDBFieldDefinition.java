package org.apache.flink.cdc.connectors.tidb.source.schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;

public class TiDBFieldDefinition {
    private String columnName;
    private String columnType;
    private boolean nullable;
    private boolean key;
    private String defaultValue;
    private String extra;
    private boolean unique;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getDefaultValue() {
        return StringUtils.isEmpty(defaultValue) ? "" : "DEFAULT " + defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isUnsigned() {
        return StringUtils.containsIgnoreCase(columnType, "unsigned");
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isKey() {
        return key;
    }

    public void setKey(boolean key) {
        this.key = key;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public String toDdl() {
        return TiDBUtils.quote(columnName)
                + " "
                + columnType
                + " "
                + (nullable ? "" : "NOT NULL");
    }
}
