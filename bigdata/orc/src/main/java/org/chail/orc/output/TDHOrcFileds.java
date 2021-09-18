package org.chail.orc.output;

import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.chail.orc.OrcField;
import org.chail.orc.OrcSchemaConverter;
import org.chail.orc.OrcSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName : TDHOrcUtils
 * @Description :
 * @Author : Chail
 * @Date: 2020-11-11 09:54
 */
public class TDHOrcFileds extends OrcSchemaConverter {

    private static final String[] tdtOrcFildName = new String[]{"operation", "originalTransaction", "bucket", "rowId", "currentTransaction", "row"};
    private static final String[] tdtOrcFildType = new String[]{"int", "bigint", "int", "bigint", "bigint", "struct"};
    private static final int transactionFileNum = 4;
    private static final String ALIAS_NAME_FLAG="_col";

    public static TypeDescription getTDHOrcTypeDescription(List<OrcField> columnFields) {
        TypeDescription typeDescription = TypeDescription.createStruct();
        for (int i = 0; i < tdtOrcFildName.length; i++) {
            String filedName = tdtOrcFildName[i];
            String filedType = tdtOrcFildType[i];
            OrcSpec.DataType i1 = OrcSpec.DataType.getDataType(OrcSchemaConverter.determineFormatTypeId(filedType));
            TypeDescription typeDescription1 = OrcSchemaConverter.determineOrcType(i1);
            //构建struct
            if (i1 == OrcSpec.DataType.STRUCT) {
                OrcSchemaConverter.buildTypeDescription(columnFields, typeDescription1);
            }
            typeDescription.addField(filedName, typeDescription1);
        }
        return typeDescription;
    }


    public static List<OrcField> createOrcOutputField(List<OrcField> columnFields) {
        return createOrcOutputField(columnFields,false);
    }


    public static List<OrcField> createOrcOutputField(List<OrcField> columnFields, boolean useAliasName) {
        List<OrcField> res = new ArrayList<>();
        for (int i = 0; i < tdtOrcFildName.length; i++) {
            String filedName = tdtOrcFildName[i];
            String filedType = tdtOrcFildType[i];
            OrcField orcField = new OrcField(filedName, filedType);
            if(useAliasName){
                orcField.setAliasName(filedName);
            }
            res.add(orcField);
            if (i == transactionFileNum) {
                break;
            }
        }

        for (int i = 0; i < columnFields.size(); i++) {
            OrcField orcField = columnFields.get(i);
            orcField.setPrimary(false);
            if(useAliasName){
                orcField.setAliasName(ALIAS_NAME_FLAG+i);
            }
            orcField.setOrcType(OrcSpec.DataType.STRUCT);
            res.add(orcField);
        }


        return res;
    }


    public static boolean isUseAliasName(TypeDescription schema) {
        boolean useAliasName = true;
        List<TypeDescription> children = schema.getChildren();
        List<String> fieldNames = schema.getFieldNames();
        if (children.size() == 6 && fieldNames.size() == 6 && fieldNames.get(5).equals(tdtOrcFildName[5])&&children.get(5).getCategory()== TypeDescription.Category.STRUCT) {
            TypeDescription typeDescription = children.get(5);
            fieldNames=typeDescription.getFieldNames();
        }
        for (int i = 0; i < fieldNames.size(); i++) {
            String s = fieldNames.get(i);
            String col = ALIAS_NAME_FLAG + i;
            if (!s.equals(col)) {
                useAliasName = false;
                break;
            }
        }
        return useAliasName;
    }

    public static boolean isUseAliasName(Reader reader) {
        return isUseAliasName(reader.getSchema());
    }

    public static String addAliasName(int index){
        return ALIAS_NAME_FLAG+index;
    }


    public static boolean isTorc(List<OrcField> inputFeildsFromSchema) {
        boolean flag = false;
        if (inputFeildsFromSchema.size() > 5) {
            for (int i = 0; i < 5; i++) {
                if (!inputFeildsFromSchema.get(i).getName().equals(tdtOrcFildName[i])) {
                    return false;
                }
            }
            flag = true;
        }
        return flag;
    }


    public static List<OrcField>  removetorcfiled(List<OrcField> inputFeildsFromSchema) {
        List<OrcField> res=new ArrayList<>();
        for (int i = 0; i < inputFeildsFromSchema.size(); i++) {
            if (i<5&&inputFeildsFromSchema.get(i).getName().equals(tdtOrcFildName[i])) {
                continue;
            }
            res.add(inputFeildsFromSchema.get(i));
        }
        return res;
    }





}
