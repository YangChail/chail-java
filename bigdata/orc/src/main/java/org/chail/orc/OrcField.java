/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.chail.orc;

import org.chail.orc.utils.TypeInfoUtils;

/**
 * @author yangc
 */
public class OrcField extends BaseFormatOutputField {

    private int orcfileIndex=-1;
    private int columnIndex;
    public static final int INDEX_LEVEL_FLAG=100000;

    /**
     * 别名
     */
    private String aliasName;

    /**
     * 是否是负载类型
     */
    private boolean isPrimary = true;

    private boolean repeating;
    private String stringFormat = "";

    public OrcField() {
    }

    /**
     * 构建 orc
     *
     * @param hiveTablecolumnName hive表字段名字
     * @param hiveTablecolumnType hive表字段类型
     */
    public OrcField(String hiveTablecolumnName, String hiveTablecolumnType) {
        try {
            TypeInfoUtils.TypeInfoParser parser = new TypeInfoUtils.TypeInfoParser(hiveTablecolumnType);
            TypeInfoUtils.TypeInfoParser.Token type = parser.expect("type");
            String text = type.text;
            setFormatType(text);
            setOrcType(text);
            setFormat(text);
            setPentahoType(OrcSchemaConverter.determineMetaType(text));
            String[] params = parser.parseParams();
            if (params.length == 1) {
                int precision = Integer.valueOf(params[0]);
                setPrecision(precision);
            } else if (params.length == 2) {
                int precision = Integer.valueOf(params[0]);
                setPrecision(precision);
                setScale(params[1]);
            }
        } catch (Exception e) {
        }
        setFormatFieldName(hiveTablecolumnName);
        setPentahoFieldName(hiveTablecolumnName);
        setName(hiveTablecolumnName);
    }

    @Override
    public void setFormatType(int formatType) {
        for (OrcSpec.DataType orcType : OrcSpec.DataType.values()) {
            if (orcType.ordinal() == formatType) {
                this.formatType = formatType;
            }
        }
    }

    public void setFormatType(String typeName) {
        try {
            setFormatType(Integer.parseInt(typeName));
        } catch (NumberFormatException nfe) {
            for (OrcSpec.DataType orcType : OrcSpec.DataType.values()) {
                if (orcType.getName().equals(typeName)) {
                    this.formatType = orcType.ordinal();
                    this.pentahoType=orcType.getPdiType();
                }
            }
        }
    }

    public boolean isDecimalType() {
        return getOrcType().getName().equals(OrcSpec.DataType.DECIMAL.getName());
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }

    public OrcSpec.DataType getOrcType() {
        return OrcSpec.DataType.values()[formatType];
    }

    public void setFormatType(OrcSpec.DataType orcType) {
        this.formatType = orcType.ordinal();
    }

    public boolean isRepeating() {
        return repeating;
    }

    public void setRepeating(boolean repeating) {
        this.repeating = repeating;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public void setOrcType(OrcSpec.DataType orcType) {
        setFormatType(orcType.getId());
    }

    public void setOrcType(String orcType) {
        for (OrcSpec.DataType tmpType : OrcSpec.DataType.values()) {
            if (tmpType.getName().equalsIgnoreCase(orcType)) {
                setFormatType(tmpType.getId());
                break;
            }
        }
    }


    public String getStringFormat() {
        return stringFormat;
    }

    public void setStringFormat(String stringFormat) {
        this.stringFormat = stringFormat == null ? "" : stringFormat;
    }


    @Override
    public String toString() {
        return "OrcField{" +
            "formatType=" + formatType +
            ", formatFieldName='" + formatFieldName + '\'' +
            ", precision=" + precision +
            ", scale=" + scale +
            '}';
    }


    public int getOrcfileIndex() {
        return orcfileIndex;
    }

    public void setOrcfileIndex(int orcfileIndex) {
        this.orcfileIndex = orcfileIndex;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }
}
