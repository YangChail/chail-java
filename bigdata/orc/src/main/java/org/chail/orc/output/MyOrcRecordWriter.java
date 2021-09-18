
package org.chail.orc.output;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.chail.orc.OrcField;
import org.chail.orc.OrcMetaDataUtil;
import org.chail.orc.OrcSchemaConverter;
import org.chail.orc.utils.OrcUtils;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.plugins.IValueMetaConverter;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yangc
 */
public class MyOrcRecordWriter implements Closeable {
    private VectorizedRowBatch batch;
    private int batchRowNumber;
    private Writer writer;
    private final IValueMetaConverter valueMetaConverter = new ValueMetaConverter();
    private RowMeta outputRowMeta = new RowMeta();
    private RowMetaAndData outputRowMetaAndData;
    private static final Logger logger = Logger.getLogger(MyOrcRecordWriter.class);
    private List<OrcField> fields;
    private TypeDescription schema;
    private boolean isTdhOrc = false;


    public MyOrcRecordWriter(List<OrcField> fields, TypeDescription schema, String filePath, boolean isTdhOrc,
                             OrcUtils fileUtil, Map<String, String> userMetadata) {
        this.fields = fields;
        this.isTdhOrc = isTdhOrc;
        /**
         *  Mutable field count
         */
        final AtomicInteger fieldNumber = new AtomicInteger();
        fields.forEach(field -> setOutputMeta(fieldNumber, field));
        outputRowMetaAndData = new RowMetaAndData(outputRowMeta, new Object[fieldNumber.get()]);
        try {
             writer = fileUtil.createOrcWirter(filePath, schema, fileUtil.getConfiguration());
            if(userMetadata!=null&&userMetadata.size()>0){
                OrcMetaDataUtil.addMetaData(writer,userMetadata);
            }
            batch = schema.createRowBatch();
        } catch (Exception e) {
            logger.error("error", e);
        }
        this.schema = schema;
    }

    private void setOutputMeta(AtomicInteger fieldNumber, OrcField field) {
        outputRowMeta.addValueMeta(getValueMetaInterface(field.getPentahoFieldName(), field.getOrcType().getPdiType()));
        fieldNumber.getAndIncrement();
    }

    /**
     * 写数据到batch钟
     * @param rowMetaAndData
     * @throws Exception
     */
    public void write(RowMetaAndData rowMetaAndData) throws Exception {
        final AtomicInteger fieldNumber = new AtomicInteger();
        batchRowNumber = batch.size++;
        int fieldsSize = fields.size();
        for ( int i=0;i<fieldsSize;i++) {
            OrcField field =fields.get(i);
            int rowMetaIndex = rowMetaAndData.getRowMeta().indexOfValue(field.getPentahoFieldName());
            int inlineType = rowMetaAndData.getRowMeta().getValueMeta(rowMetaIndex).getType();
            int fieldNo = fieldNumber.getAndIncrement();
            ColumnVector columnVector = batch.cols[fieldNo];
            if (rowMetaAndData.getData()[rowMetaIndex] == null) {
                if (field.getAllowNull()) {
                    columnVector.isNull[batchRowNumber] = true;
                    columnVector.noNulls = false;
                    return;
                }
            }
            columnVector.isNull[batchRowNumber] = false;
            Object inlineValue = rowMetaAndData.getData()[rowMetaIndex];
            Object setValue = null;
            try {
                setValue = valueMetaConverter.convertFromSourceToTargetDataType(inlineType, field.getOrcType().getPdiType(),
                    inlineValue);
            } catch (ValueMetaConversionException e) {
                logger.error(e);
            }
            outputRowMetaAndData.getData()[rowMetaIndex] = setValue;
            if (field.isPrimary()) {
                addData(columnVector, field, rowMetaAndData, rowMetaIndex);
            } else {
                StructColumnVector structColumnVector = (StructColumnVector) columnVector;
                ColumnVector[] columnVectors = structColumnVector.fields;
                for (int k = 0; k < columnVectors.length; k++) {
                    ColumnVector field1 = columnVectors[k];
                    addData(field1, field, rowMetaAndData, rowMetaIndex);
                    i++;
                    if(i<fieldsSize){
                        field =fields.get(i);
                        rowMetaIndex++;
                    }

                }
            }
        }
        if (batch.size == batch.getMaxSize() - 1) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }

    private void addData(ColumnVector columnVector, OrcField field, RowMetaAndData rowMetaAndData, int rowMetaIndex) {
        String format = field.getFormat();
        switch ( OrcSchemaConverter.determineFormatType(format)) {
            case BOOLEAN:
                try {
                    ((LongColumnVector) columnVector).vector[batchRowNumber] = rowMetaAndData.getBoolean(
                        field.getPentahoFieldName(),
                        field.getDefaultValue() != null ? Boolean.valueOf(field.getDefaultValue()) : false) ? 1L : 0L;
                } catch (KettleValueException e) {
                    logger.error(e);
                }
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                try {
                    ((LongColumnVector) columnVector).vector[batchRowNumber] = rowMetaAndData.getInteger(
                        field.getPentahoFieldName(),
                        field.getDefaultValue() != null ? Long.valueOf(field.getDefaultValue()) : 0);
                } catch (KettleValueException e) {
                    logger.error(e);
                }
                break;
            case BINARY:
                try {
                    setBytesColumnVector(((BytesColumnVector) columnVector),
                        rowMetaAndData.getBinary(field.getPentahoFieldName(),
                            field.getDefaultValue() != null ? field.getDefaultValue().getBytes() : new byte[0]));
                } catch (KettleValueException e) {
                    logger.error(e);
                }
                break;
            case FLOAT:
            case DOUBLE:
                try {
                    double number = rowMetaAndData.getNumber(field.getPentahoFieldName(),
                        field.getDefaultValue() != null ? new Double(field.getDefaultValue()) : new Double(0));
                    number = applyScale(number, field);
                    ((DoubleColumnVector) columnVector).vector[batchRowNumber] = number;
                } catch (KettleValueException e) {
                    logger.error(e);
                }
                break;
            case DECIMAL:
                try {
                    BigDecimal bi = rowMetaAndData.getBigNumber(field.getPentahoFieldName(),
                        field.getDefaultValue() != null ? new BigDecimal(field.getDefaultValue())
                            : new BigDecimal(0));
                    HiveDecimal hiveDecimal = HiveDecimal.create(bi);
                    ((DecimalColumnVector) columnVector).vector[batchRowNumber] = new HiveDecimalWritable(hiveDecimal
                    );
                } catch (KettleValueException e) {
                    logger.error(e);
                }
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
                try {
                    setBytesColumnVector(((BytesColumnVector) columnVector), rowMetaAndData.getString(
                        field.getPentahoFieldName(), field.getDefaultValue() != null ? field.getDefaultValue() : ""));
                } catch (KettleValueException e) {
                    logger.error(e);
                }
                break;
            case DATE:
                try {
                    String conversionMask = rowMetaAndData.getValueMeta(rowMetaIndex).getConversionMask();
                    if (conversionMask == null) {
                        conversionMask = ValueMetaBase.DEFAULT_DATE_PARSE_MASK;
                    }
                    DateFormat dateFormat = new SimpleDateFormat(conversionMask);
                    Date defaultDate = field.getDefaultValue() != null ? dateFormat.parse(field.getDefaultValue())
                        : new Date(0);
                    Date date = rowMetaAndData.getDate(field.getPentahoFieldName(), defaultDate);
                    ((LongColumnVector) columnVector).vector[batchRowNumber] = getOrcDate(date,
                        rowMetaAndData.getValueMeta(rowMetaIndex).getDateFormatTimeZone());
                } catch (KettleValueException | ParseException e) {
                    logger.error(e);
                }
                break;
            case TIMESTAMP:
                try {
                    String conversionMask = rowMetaAndData.getValueMeta(rowMetaIndex).getConversionMask();
                    if (conversionMask == null) {
                        conversionMask = ValueMetaBase.DEFAULT_DATE_PARSE_MASK;
                    }
                    DateFormat dateFormat = new SimpleDateFormat(conversionMask);
                    ((TimestampColumnVector) columnVector).set(batchRowNumber, new Timestamp(rowMetaAndData.getDate(
                        field.getPentahoFieldName(),
                        field.getDefaultValue() != null ? (dateFormat.parse(field.getDefaultValue())) : new Date(0))
                        .getTime()));
                } catch (KettleValueException | ParseException e) {
                    logger.error(e);
                }
                break;
            default:
                throw new RuntimeException(
                    "Field: " + field.getDefaultValue() + "  Undefined type: " + field.getOrcType().getName());
        }
    }


    private double applyScale(double number, OrcField outputField) {
        if (outputField.getScale() > 0) {
            BigDecimal bd = BigDecimal.valueOf(number);
            bd = bd.setScale(outputField.getScale(), BigDecimal.ROUND_HALF_UP);
            number = bd.doubleValue();
        }
        return number;
    }

    private int getOrcDate(Date date, TimeZone timeZone) {
        if (timeZone == null) {
            timeZone = TimeZone.getDefault();
        }
        LocalDate rowDate = date.toInstant().atZone(timeZone.toZoneId()).toLocalDate();
        return Math.toIntExact(ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), rowDate));
    }

    private void setBytesColumnVector(BytesColumnVector bytesColumnVector, String value) {
        if (value == null) {
            setBytesColumnVector(bytesColumnVector, new byte[0]);
        } else {
            setBytesColumnVector(bytesColumnVector, value.getBytes());
        }
    }

    private void setBytesColumnVector(BytesColumnVector bytesColumnVector, byte[] value) {
        bytesColumnVector.vector[batchRowNumber] = value;
        bytesColumnVector.start[batchRowNumber] = 0;
        bytesColumnVector.length[batchRowNumber] = value.length;
    }

    @Override
    public void close() throws IOException {
        if (batch.size > 0) {
            writer.addRowBatch(batch);
        }
        writer.close();
    }

    private ValueMetaInterface getValueMetaInterface(String fieldName, int fieldType) {
        switch (fieldType) {
            case ValueMetaInterface.TYPE_INET:
                return new ValueMetaInternetAddress(fieldName);
            case ValueMetaInterface.TYPE_STRING:
                return new ValueMetaString(fieldName);
            case ValueMetaInterface.TYPE_INTEGER:
                return new ValueMetaInteger(fieldName);
            case ValueMetaInterface.TYPE_NUMBER:
                return new ValueMetaNumber(fieldName);
            case ValueMetaInterface.TYPE_BIGNUMBER:
                return new ValueMetaBigNumber(fieldName);
            case ValueMetaInterface.TYPE_TIMESTAMP:
                return new ValueMetaTimestamp(fieldName);
            case ValueMetaInterface.TYPE_DATE:
                return new ValueMetaDate(fieldName);
            case ValueMetaInterface.TYPE_BOOLEAN:
                return new ValueMetaBoolean(fieldName);
            case ValueMetaInterface.TYPE_BINARY:
                return new ValueMetaBinary(fieldName);
        }
        return null;
    }

}
