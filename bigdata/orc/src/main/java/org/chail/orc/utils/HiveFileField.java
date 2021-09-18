package org.chail.orc.utils;

import org.apache.hadoop.hive.common.type.HiveDecimal;


public class HiveFileField {

    private String colName;

    private HiveType colType;

    private String colTypeStr;

    private int precision = HiveDecimal.USER_DEFAULT_PRECISION;
    private int scale = HiveDecimal.USER_DEFAULT_SCALE;

    public HiveFileField(String colName, String colType) {
        this.colTypeStr = colType;
        String[] params = TypeInfoUtils.getParams(colType);
        try {
            if (params.length == 1) {
                this.precision = Integer.valueOf(params[0]);
            } else if (params.length == 2) {
                this.precision = Integer.valueOf(params[0]);
                this.scale = Integer.valueOf(params[1]);
            }
        } catch (Exception e) {

        }
        this.colType = HiveType.getTypeByValue(colType);
        this.colName = colName;
    }

    public HiveFileField(String colName, HiveType colType) {
        this.colType = colType;
        this.colName = colName;
    }

    public HiveFileField() {
    }

    public HiveType getColType() {
        return colType;
    }

    public void setColType(HiveType colType) {
        this.colType = colType;
    }

    public void setColType(String colType) {
        this.colType = HiveType.getTypeByValue(colType);
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }


    public static Object getRowMetaValue(String value, HiveType type) {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case INTEGER:
                try {
                    return Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    return value;
                }
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


    public String getColTypeStr() {
        return colTypeStr;
    }

    public void setColTypeStr(String colTypeStr) {
        this.colTypeStr = colTypeStr;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * 解析hive类型
     * @param fieldName
     * @param typeString
     * @return
     */
    public static HiveFileField getHiveFileField(String fieldName, String typeString) {
        HiveFileField hiveFileField = new HiveFileField();
        hiveFileField.setColName(fieldName);
        hiveFileField.setColTypeStr(typeString);

        try {
            TypeInfoUtils.TypeInfoParser parser = new TypeInfoUtils.TypeInfoParser(typeString);
            TypeInfoUtils.TypeInfoParser.Token type = parser.expect("type");
            String[] params = parser.parseParams();
            if (params.length == 1) {
                int precision = Integer.valueOf(params[0]);
                hiveFileField.setPrecision(precision);
            } else if (params.length == 2) {
                int precision = Integer.valueOf(params[0]);
                int scale = Integer.valueOf(params[1]);
                hiveFileField.setPrecision(precision);
                hiveFileField.setScale(scale);
            }
            hiveFileField.setColType(type.text);
        } catch (Exception e) {
        }
        return hiveFileField;
    }
}
