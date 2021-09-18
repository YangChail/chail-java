
package org.chail.orc.input;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.chail.orc.OrcField;
import org.chail.orc.output.TDHOrcFileds;
import org.chail.orc.utils.OrcUtils;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaNone;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author yangc
 */
public class MyOrcRecordReader implements Closeable {
    private  List<OrcField> dialogInputFields;
    private VectorizedRowBatch batch;
    private RecordReader recordReader;
    private int currentBatchRow;

    /**
     * orc序号
     */
    private OrcRecordConverter orcConverter = null;

    public MyOrcRecordReader(String fileName, OrcUtils fileUtil, List<OrcField> dialogInputFields, boolean isTorc) {

        Reader reader = null;
        try {
            reader = fileUtil.getOrcReard(fileName, fileUtil.getConfiguration());
            recordReader = reader.rows();
            TypeDescription schema = reader.getSchema();
            doFilterColumn(reader,dialogInputFields,isTorc);
            OrcMetaDataReader orcMetaDataReader = new OrcMetaDataReader(reader);
            orcMetaDataReader.read(  this.dialogInputFields);
            batch = schema.createRowBatch();
            //设置元数据
            orcConverter = new OrcRecordConverter( setMetadata( this.dialogInputFields));
            setNextBatch();
        } catch (Exception e) {
            throw new IllegalArgumentException("No rows to read in " + fileName, e);
        }
    }


    /**
     * 处理文件里面的字段与用户字段不一致问题
     */
    private void doFilterColumn(  Reader reader,List<OrcField> dialogInputFields,boolean isTorc){
        //
        TypeDescription schema = reader.getSchema();
        //判断数据库是否使用别名
        boolean useAliasName= TDHOrcFileds.isUseAliasName(schema);
        for(OrcField orcField:dialogInputFields){
           if(StringUtils.isEmpty(orcField.getAliasName())){
               useAliasName=false;
               break;
           }
        }
        dialogInputFields = isTorc? TDHOrcFileds.createOrcOutputField(dialogInputFields,useAliasName):dialogInputFields;
        //如果是tdh orc 会展开 orc struct 里面的字段信息
        Map<String, OrcField> collect =useAliasName?dialogInputFields.stream().collect
            (Collectors.toMap(OrcField::getAliasName, Function.identity())):dialogInputFields.stream().collect
            (Collectors.toMap(OrcField::getFormatFieldName, Function.identity()));
        getFiledsMap(collect,schema,0);
        this.dialogInputFields=dialogInputFields;

    }

    public void getFiledsMap( Map<String, OrcField>   orcFields ,TypeDescription typeDescription,int level) {
        List<TypeDescription> children = typeDescription.getChildren();
        List<String> fieldNames = typeDescription.getFieldNames();
        int i1 = level * OrcField.INDEX_LEVEL_FLAG;
        for (int i = 0; i < children.size(); i++) {
            TypeDescription c = children.get(i);
            if (c.getCategory() == TypeDescription.Category.STRUCT) {
                getFiledsMap(orcFields,c,++level);
            }else{
                String name = fieldNames.get(i);
                OrcField orcField = orcFields.get(name);
                i1++;
                if(orcField!=null){
                    orcField.setOrcfileIndex(i1);
                }
            }
        }
    }



    private boolean setNextBatch() throws IOException {
        currentBatchRow = 0;
        return recordReader.nextBatch(batch);
    }


    public Iterator<RowMetaAndData> iterator() {
        return new Iterator<RowMetaAndData>() {

            @Override
            public boolean hasNext() {
                if (currentBatchRow < batch.size) {
                    return true;
                }
                try {
                    return setNextBatch();
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            @Override
            public RowMetaAndData next() {
                RowMetaAndData rowMeta = orcConverter.convertFromOrc(batch, currentBatchRow, dialogInputFields );
                currentBatchRow++;
                return rowMeta;
            }
        };
    }

    @Override
    public void close() throws IOException {
        recordReader.close();
    }


    private  List<ValueMetaInterface> setMetadata(List<OrcField> dialogInputFields){
        List<ValueMetaInterface> valueMetaList = new ArrayList<ValueMetaInterface>();
        //设置元数据
        for (OrcField inputField : dialogInputFields) {
            String pentahoFieldName = inputField.getPentahoFieldName();
            int pentahoType = inputField.getPentahoType();
            ValueMetaInterface v;
            try {
                v = ValueMetaFactory.createValueMeta(pentahoFieldName, pentahoType);
            } catch (KettlePluginException e) {
                v = new ValueMetaNone(pentahoFieldName);
            }
            valueMetaList.add(v);
        }
        return valueMetaList;

    }

}
