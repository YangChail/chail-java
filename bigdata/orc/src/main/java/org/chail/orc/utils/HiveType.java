package org.chail.orc.utils;

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;

public enum HiveType {
    /**
     * 整数
     */
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    INT("INT"),
    INTEGER("INTEGER"),
    BIGINT("BIGINT"),
    /**
     * 单精度浮点数
     */
    FLOAT("FLOAT"),
    /**
     * 双精度浮点数
     */
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    NUMERIC("NUMERIC"),
    /**
     * 日期
     */
    TIMESTAMP("TIMESTAMP"),
    DATE("DATE"),
    /**
     * 字符串
     */
    STRING("STRING"),
    VARCHAR("VARCHAR"),
    CHAR("CHAR"),
    /**
     * 布尔
     */
    BOOLEAN("BOOLEAN");

    private String value;

    private HiveType(String value) {
        this.value = value;
    }

    /**
     * hive类型转kettle类型
     * @param fields
     * @return
     */
    public static RowMeta getRowMeta(HiveFileField[] fields) {
        RowMeta rowMeta = new RowMeta();
        for (HiveFileField field : fields) {
            switch (field.getColType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case INTEGER:
                case BIGINT:
                    rowMeta.addValueMeta(getValueMeta(field.getColName(), ValueMetaInterface.TYPE_INTEGER));
                    break;
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                case NUMERIC:
                    rowMeta.addValueMeta(getValueMeta(field.getColName(), ValueMetaInterface.TYPE_NUMBER));
                    break;
                case TIMESTAMP:
                case DATE:
                case STRING:
                case CHAR:
                case VARCHAR:
                    rowMeta.addValueMeta(getValueMeta(field.getColName(), ValueMetaInterface.TYPE_STRING));
                    break;
                case BOOLEAN:
                    rowMeta.addValueMeta(getValueMeta(field.getColName(), ValueMetaInterface.TYPE_BOOLEAN));
                    break;
                    default:
                        rowMeta.addValueMeta(getValueMeta(field.getColName(), ValueMetaInterface.TYPE_STRING));
                        break;
            }
        }
        return rowMeta;
    }


    private static ValueMetaInterface getValueMeta(String name, int type) {
        ValueMetaInterface value = null;
        try {
            value = ValueMetaFactory.createValueMeta(name, type);
            value.setOrigin(name);
        } catch (KettlePluginException e) {
            e.printStackTrace();
        }
        return value;
    }

    /**
     * 用字符串获取type
     * @param type
     * @return
     */
    public static HiveType getTypeByValue(String type) {
        HiveType[] types = values();
        if (type.contains("(")) {
            if (type.toUpperCase().startsWith("VARCHAR")) {
                return VARCHAR;
            }
            if (type.toUpperCase().startsWith("CHAR")){
                return CHAR;
            }
            if (type.toUpperCase().startsWith("DECIMAL")) {
                return DECIMAL;
            }
            if (type.toUpperCase().startsWith("NUMERIC")) {
                return NUMERIC;
            }
        }
        if ("long".equalsIgnoreCase(type)) {
            return BIGINT;
        }
        for (HiveType hiveType : types) {
            if (hiveType.value.equalsIgnoreCase(type)){
                return hiveType;
            }
        }
        return null;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public static Object getRowMetaValue(String value, HiveType type) {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case INTEGER:
            case BIGINT:
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException e) {
                    return value;
                }
            case FLOAT:
                try {
                    return Float.parseFloat(value);
                } catch (NumberFormatException e) {
                    return value;
                }
            case DOUBLE:
            case DECIMAL:
            case NUMERIC:
                try {
                    return Double.valueOf(value);
                } catch (NumberFormatException e) {
                    return value;
                }
            case TIMESTAMP:
            case DATE:
            case STRING:
            case CHAR:
            case VARCHAR:
                return value;
            case BOOLEAN:
                try {
                    return Boolean.valueOf(value);
                } catch (Exception e) {
                    return value;
                }
            default:
                return value;
        }
    }



}
