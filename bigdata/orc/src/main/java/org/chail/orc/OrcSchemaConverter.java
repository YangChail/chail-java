/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.lang.StringUtils;
import org.apache.orc.TypeDescription;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName : OrcSchemaConverter
 * @Description :
 * @Author : Chail
 * @Date: 2020-11-11 09:54
 */
public class OrcSchemaConverter {


    /**
     * 构建表头
     *
     * @param fields
     * @return
     */
    public static TypeDescription buildTypeDescription(List<OrcField> fields) {
        TypeDescription typeDescription = TypeDescription.createStruct();
        fields.forEach(field -> addStructField(typeDescription, field));
        return typeDescription;
    }

    /**
     * 增加表头
     *
     * @param fields
     * @param typeDescription
     * @return
     */
    public static TypeDescription buildTypeDescription(List<OrcField> fields, TypeDescription typeDescription) {
        fields.forEach(field -> addStructField(typeDescription, field));
        return typeDescription;
    }


    /**
     * 增加表头
     *
     * @param typeDescription
     * @param field
     */
    public static void addStructField(TypeDescription typeDescription, OrcField field) {
        String format = field.getFormat();
        OrcSpec.DataType dataType = determineFormatType(format);
        TypeDescription determineOrcType = determineOrcType(dataType);
        //解决长度丢失，hive char(xx),varchar(xx)查询不出来的问题
        if ((field.getOrcType() == OrcSpec.DataType.CHAR || field.getOrcType() == OrcSpec.DataType.VARCHAR)
            && field.getMaxLength() > 0) {
            determineOrcType.withMaxLength(field.getMaxLength());
        }
        if (StringUtils.isNotEmpty(field.getAliasName())) {
            typeDescription.addField(field.getAliasName(), determineOrcType);
        } else {
            typeDescription.addField(field.getFormatFieldName(), determineOrcType);
        }

    }


    /**
     * orc  转kettle类型字段
     *
     * @param typeDescription
     * @return
     */
    public static List<OrcField> buildInputFields(TypeDescription typeDescription) {
        List<OrcField> inputFields = new ArrayList<OrcField>();
        Iterator fieldNameIterator = typeDescription.getFieldNames().iterator();
        for (TypeDescription subDescription : typeDescription.getChildren()) {
            // Assume getFieldNames is 1:1 with getChildren
            String fieldName = (String) fieldNameIterator.next();
            int formatType = determineFormatTypeId(subDescription);
            //解决星环 T-Orc复杂类型问题
            if (formatType == OrcSpec.DataType.STRUCT.getId()) {
                //递归调用增加字段
                List<OrcField> orcInputFields = buildInputFields(subDescription);
                orcInputFields.forEach(ob -> {
                    ob.setOrcType(OrcSpec.DataType.STRUCT);
                });
                inputFields.addAll(orcInputFields);
                continue;
            }
            // Skip orc types we do not support
            if (formatType != -1) {
                int metaType = determineMetaType(subDescription);
                if (metaType == -1) {
                    throw new IllegalStateException("Orc Field Name: " + fieldName
                        + " - Could not find pdi field type for " + subDescription.getCategory().getName());
                }

                OrcField inputField = new OrcField();
                inputField.setFormatFieldName(fieldName);
                inputField.setFormatType(formatType);
                inputField.setPentahoType(metaType);
                inputField.setPentahoFieldName(fieldName);
                inputFields.add(inputField);
            }
        }
        return inputFields;
    }

    public static int determineMetaType(TypeDescription subDescription) {
        return determineMetaType(subDescription.getCategory().getName());
    }


    public static int determineMetaType(String name) {
        switch (name) {
            case "string":
            case "char":
            case "varchar":
                return ValueMetaInterface.TYPE_STRING;
            case "bigint":
            case "tinyint":
            case "smallint":
            case "int":
                return ValueMetaInterface.TYPE_INTEGER;
            case "double":
            case "float":
                return ValueMetaInterface.TYPE_NUMBER;
            case "decimal":
                return ValueMetaInterface.TYPE_BIGNUMBER;
            case "timestamp":
                return ValueMetaInterface.TYPE_TIMESTAMP;
            case "date":
                return ValueMetaInterface.TYPE_DATE;
            case "boolean":
                return ValueMetaInterface.TYPE_BOOLEAN;
            case "binary":
                return ValueMetaInterface.TYPE_BINARY;
            default:
                return -1;
        }

    }



    public static OrcSpec.DataType determineFormatType(String subDescription) {
        switch (subDescription) {
            case "string":
                return OrcSpec.DataType.STRING;
            case "char":
                return OrcSpec.DataType.CHAR;
            case "varchar":
                return OrcSpec.DataType.VARCHAR;
            case "bigint":
                return OrcSpec.DataType.BIGINT;
            case "float":
                return OrcSpec.DataType.FLOAT;
            case "double":
                return OrcSpec.DataType.DOUBLE;
            case "decimal":
                return OrcSpec.DataType.DECIMAL;
            case "timestamp":
                return OrcSpec.DataType.TIMESTAMP;
            case "date":
                return OrcSpec.DataType.DATE;
            case "boolean":
                return OrcSpec.DataType.BOOLEAN;
            case "binary":
                return OrcSpec.DataType.BINARY;
            case "int":
                return OrcSpec.DataType.INTEGER;
            case "tinyint":
                return OrcSpec.DataType.TINYINT;
            case "smallint":
                return OrcSpec.DataType.SMALLINT;
            case "struct":
                return OrcSpec.DataType.STRUCT;
            default:
                return null;
        }


    }

    public static int determineFormatTypeId(String subDescription) {
        OrcSpec.DataType dataType = determineFormatType(subDescription);
        if(dataType==null){
            return -1;
        }
        return dataType.getId();

    }


    public static int determineFormatTypeId(TypeDescription subDescription) {
        return determineFormatTypeId(subDescription.getCategory().getName());
    }

    public static TypeDescription determineOrcType(OrcSpec.DataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return TypeDescription.createBoolean();
            case TINYINT:
                return TypeDescription.createByte();
            case SMALLINT:
                return TypeDescription.createShort();
            case INTEGER:
                return TypeDescription.createInt();
            case BIGINT:
                return TypeDescription.createLong();
            case DATE:
                return TypeDescription.createDate();
            case BINARY:
                return TypeDescription.createBinary();
            case CHAR:
                return TypeDescription.createChar();
            case VARCHAR:
                return TypeDescription.createVarchar();
            case STRING:
                return TypeDescription.createString();
            case FLOAT:
                return TypeDescription.createFloat();
            case DOUBLE:
                return TypeDescription.createDouble();
            case DECIMAL:
                return TypeDescription.createDecimal();
            case TIMESTAMP:
                return TypeDescription.createTimestamp();
            case STRUCT:
                return TypeDescription.createStruct();
            default:
                throw new RuntimeException("Attempted to write an unsupported Orc type: " + dataType.getName());
        }
    }
}
