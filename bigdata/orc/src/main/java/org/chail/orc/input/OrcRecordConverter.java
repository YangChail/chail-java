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
package org.chail.orc.input;

import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.log4j.Logger;
import org.chail.orc.OrcField;
import org.chail.orc.OrcSchemaConverter;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaConversionException;
import org.pentaho.di.core.row.value.ValueMetaConverter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

/**
 * @author yangc
 */
public class OrcRecordConverter {
	private ValueMetaConverter valueMetaConverter = new ValueMetaConverter();
	private static final Logger logger = Logger.getLogger(OrcRecordConverter.class);
	private List<ValueMetaInterface> valueMetaList = new ArrayList<ValueMetaInterface>();


    public OrcRecordConverter(List<ValueMetaInterface> valueMetaList) {
        this.valueMetaList = valueMetaList;
    }


    private void getColumnVectorMapIndex(   Map<Integer,ColumnVector> map,ColumnVector[] cols ,int level){
        int i1 = level * OrcField.INDEX_LEVEL_FLAG;
        for(int i=0;i<cols.length;i++){
            ColumnVector columnVector = cols[i];
            if(columnVector instanceof  StructColumnVector) {
                StructColumnVector vector = (StructColumnVector) columnVector;
                ColumnVector[] fields = vector.fields;
                getColumnVectorMapIndex(map,fields,++level);
            }else{
                map.put(++i1,columnVector);
            }
        }


    }


    /**
     * 循环类型orc里面的数据，以dialogInputFields为准
     * @param batch
     * @param currentBatchRow
     * @param dialogInputFields
     * @return
     */
	public RowMetaAndData convertFromOrc(VectorizedRowBatch batch, int currentBatchRow,List<OrcField> dialogInputFields) {
        RowMetaAndData rowMetaAndData=new RowMetaAndData();
        //读取所有 页面组装的字段信息
        //防止错位
        // 文件字段信息
        Map<Integer,ColumnVector> map=new HashMap<>();
        getColumnVectorMapIndex(map,batch.cols,0);
        for (int i = 0; i < dialogInputFields.size(); i++) {
            OrcField inputField = dialogInputFields.get(i);
            Object value =null;
            if (inputField != null) {
                int orcfileIndex = inputField.getOrcfileIndex();
                ColumnVector columnVector = map.get(orcfileIndex);
                if(columnVector!=null){
                    if(inputField.getPentahoType()<1){
                      inputField.setPentahoType(OrcSchemaConverter.determineMetaType(inputField.getFormat()));
                    }
                    value =convertFromSourceToTargetDataType(columnVector,currentBatchRow, inputField.getPentahoType());
                    coverData(inputField,inputField,value,rowMetaAndData,i);
                }else{
                    rowMetaAndData.addValue(valueMetaList.get(i), null);
                    continue;
                }
            }
        }
		return rowMetaAndData;

	}


    /**
     * 解析ORC 对象具体  数据
     * @param columnVector
     * @param currentBatchRowOld
     * @param orcValueMetaInterface
     * @return
     */
	protected static Object convertFromSourceToTargetDataType(ColumnVector columnVector, int currentBatchRowOld,
			int orcValueMetaInterface) {
		int currentBatchRow = currentBatchRowOld;
		if (columnVector.isRepeating) {
			currentBatchRow = 0;
		}
		if (columnVector.isNull[currentBatchRow]) {
			return null;
		}
		switch (orcValueMetaInterface) {
		case ValueMetaInterface.TYPE_INET:
			try {
				return InetAddress.getByName(new String(((BytesColumnVector) columnVector).vector[currentBatchRow],
						((BytesColumnVector) columnVector).start[currentBatchRow],
						((BytesColumnVector) columnVector).length[currentBatchRow]));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			return null;
		case ValueMetaInterface.TYPE_STRING:
			try {
				byte[] bytes = ((BytesColumnVector) columnVector).vector[currentBatchRow];
				if (bytes != null) {
					return new String(bytes, ((BytesColumnVector) columnVector).start[currentBatchRow],
							((BytesColumnVector) columnVector).length[currentBatchRow]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		case ValueMetaInterface.TYPE_INTEGER:
			return (long) ((LongColumnVector) columnVector).vector[currentBatchRow];

		case ValueMetaInterface.TYPE_NUMBER:
			return ((DoubleColumnVector) columnVector).vector[currentBatchRow];

		case ValueMetaInterface.TYPE_BIGNUMBER:
			HiveDecimalWritable obj = ((DecimalColumnVector) columnVector).vector[currentBatchRow];
			return obj.getHiveDecimal().bigDecimalValue();

		case ValueMetaInterface.TYPE_TIMESTAMP:
			Timestamp timestamp = new Timestamp(((TimestampColumnVector) columnVector).time[currentBatchRow]);
			timestamp.setNanos(((TimestampColumnVector) columnVector).nanos[currentBatchRow]);
			return timestamp;

		case ValueMetaInterface.TYPE_DATE:
			LocalDate localDate = LocalDate.ofEpochDay(0)
					.plusDays(((LongColumnVector) columnVector).vector[currentBatchRow]);
			Date dateValue = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
			return dateValue;

		case ValueMetaInterface.TYPE_BOOLEAN:
            return ((LongColumnVector) columnVector).vector[currentBatchRow] == 0 ? false : true;

		case ValueMetaInterface.TYPE_BINARY:
			byte[] origBytes = ((BytesColumnVector) columnVector).vector[currentBatchRow];
			int startPos = ((BytesColumnVector) columnVector).start[currentBatchRow];
			byte[] newBytes = Arrays.copyOfRange(origBytes, startPos,
					startPos + ((BytesColumnVector) columnVector).length[currentBatchRow]);
			return newBytes;
            default:
                return null;
		}
	}


    /**
     * 获取类型
     * @param formatFieldName
     * @param fields
     * @return
     */
	public OrcField getFormatField(String formatFieldName, List< OrcField> fields) {
		if (formatFieldName == null || formatFieldName.trim().isEmpty()) {
			return null;
		}
		for (OrcField field : fields) {
			if (field.getFormatFieldName().equals(formatFieldName)) {
				return field;
			}
		}

		return null;
	}


    /**
     * 转换数据
     * @param inputField
     * @param orcField
     * @param value
     * @param rowMetaAndData
     */
    private void coverData( OrcField inputField , OrcField orcField,Object value, RowMetaAndData rowMetaAndData,int index){
        Object convertToSchemaValue = null;
        try {
            String dateFormatStr = inputField.getStringFormat();
            if ((dateFormatStr == null) || (dateFormatStr.trim().length() == 0)) {
                dateFormatStr = ValueMetaBase.DEFAULT_DATE_FORMAT_MASK;
            }
            valueMetaConverter.setDatePattern(new SimpleDateFormat(dateFormatStr));
            convertToSchemaValue = valueMetaConverter.convertFromSourceToTargetDataType(
                orcField.getPentahoType(), inputField.getPentahoType(), value);
        } catch (ValueMetaConversionException e) {
            logger.error(e);
        }
        rowMetaAndData.addValue(valueMetaList.get(index), convertToSchemaValue);
        String stringFormat = inputField.getStringFormat();
        if ((stringFormat != null) && (stringFormat.trim().length() > 0)) {
            rowMetaAndData.getValueMeta(rowMetaAndData.size() - 1).setConversionMask(stringFormat);
        }
    }

}
