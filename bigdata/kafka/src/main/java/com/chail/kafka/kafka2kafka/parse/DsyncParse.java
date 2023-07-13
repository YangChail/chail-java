package com.chail.kafka.kafka2kafka.parse;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mchz.mcdatasource.utils.CollectionUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * @author : yangc
 * @date :2023/7/12 10:19
 * @description :
 * @modyified By:
 */
@Slf4j
public class DsyncParse {


    public static List<Record> parseFrom(Object obj) {
        if (!(obj instanceof byte[])) {
            throw new IllegalArgumentException("不支持的参数类型, need: byte[], actual: " + obj.getClass().getName());
        }
        byte[] rowData = (byte[]) obj;
        List<Record> resultList = new LinkedList<>();
        try {
            DSyncPacket.Messages messages = DSyncPacket.Messages.parseFrom(rowData);
            List<ByteString> messagesList = messages.getMessagesList();
            for (int i = 0; i < messagesList.size(); i++) {
                ByteString string = messagesList.get(i);
                DSyncEntry.Entry entry = DSyncEntry.Entry.parseFrom(string);
                List<List<ValueColumn>> valueList = getModelFromEntry(entry);
                if (CollectionUtils.isEmpty(valueList)) {
                    continue;
                }
                ValueColumn sequenceValueColumn = new ValueColumn(IncrementConstant.SEQUENCE, Type.longType());
                sequenceValueColumn.setData(new Value(i));
                for (List<ValueColumn> valueColumnList : valueList) {
                    valueColumnList.add(sequenceValueColumn);
                    Record record = new DefaultRecord();
                    valueColumnList.forEach(record::addColumn);
                    resultList.add(record);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("parse error", e);
        }
        return resultList;
    }

    private final static Set<DSyncEntry.EventType> SUPPORT_TYPES = new HashSet<>(Arrays.asList(DSyncEntry.EventType.INSERT, DSyncEntry.EventType.UPDATE, DSyncEntry.EventType.DELETE, DSyncEntry.EventType.TRUNCATE, DSyncEntry.EventType.ROW_CHAIN));

    protected static List<List<ValueColumn>> getModelFromEntry(DSyncEntry.Entry entry) {
        DSyncEntry.RowChange rowChange;
        try {
            rowChange = DSyncEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            log.error("parse error", e);
            return new ArrayList<>(0);
        }

        DSyncEntry.EventType eventType = entry.getHeader().getEventType();
        boolean ddl = entry.getHeader().getIsDdl() && eventType != DSyncEntry.EventType.TRUNCATE;
        if (ddl) {
           return null;
        } else {
            if (!SUPPORT_TYPES.contains(eventType)) {
                List<ValueColumn> result = new ArrayList<>(16);
                ValueColumn operationTypeColumn = new ValueColumn(IncrementConstant.OPERATION, Type.stringType());
                operationTypeColumn.setData(new Value(DataOperaType.OTHERS));
                result.add(operationTypeColumn);
                return Collections.singletonList(result);
            }
            return analyzeDml(entry, rowChange);
        }
    }

    /**
     * DML解析
     *
     * @param entry
     * @param rowChange
     * @return
     */
    private static List<List<ValueColumn>> analyzeDml(DSyncEntry.Entry entry, DSyncEntry.RowChange rowChange) {
        List<List<ValueColumn>> resultList = new ArrayList<>(16);
        List<DSyncEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        if (rowDatasList == null) {
            return new ArrayList<>(0);
        }
        // 解析具体数据
        if (CollectionUtils.isEmpty(rowDatasList)) {
            DSyncEntry.RowData rowData = DSyncEntry.RowData.newBuilder().build();
            List<ValueColumn> rowList = analyzeRowData(rowData, entry);
            resultList.add(rowList);
        } else {
            for (DSyncEntry.RowData rowData : rowDatasList) {
                List<ValueColumn> rowList = analyzeRowData(rowData, entry);
                resultList.add(rowList);
            }
        }
        return resultList;
    }

    private static List<ValueColumn> analyzeRowData(DSyncEntry.RowData rowData, DSyncEntry.Entry entry) {
        DSyncEntry.EventType eventType = entry.getHeader().getEventType();
        List<DSyncEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
        List<DSyncEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
        // 缓存
        List<ValueColumn> resultList = new LinkedList<>();
        if (CollectionUtils.isNotEmpty(afterColumnsList)) {
            for (DSyncEntry.Column column : afterColumnsList) {
                // TODO 先随便设置一个
                ValueColumn valueColumn = new ValueColumn(column.getName(), Type.stringType());
                valueColumn.setData(new Value(getColumnValue(column), column.getIsNull()));
                resultList.add(valueColumn);
            }
        }
        if (CollectionUtils.isNotEmpty(beforeColumnsList)) {
            for (DSyncEntry.Column column : beforeColumnsList) {
                Optional<ValueColumn> optional = resultList.stream().filter(c -> c.getName().equals(column.getName())).findFirst();
                if (optional.isPresent()) {
                    optional.get().setBeforeData(new Value(getColumnValue(column), column.getIsNull()));
                } else {
                    ValueColumn valueColumn = new ValueColumn(column.getName(), Type.stringType());
                    valueColumn.setBeforeData(new Value(getColumnValue(column), column.getIsNull()));
                    valueColumn.setData(valueColumn.getBeforeData());
                    resultList.add(valueColumn);
                }
            }
        }


        DataOperaType operationType;
        switch (eventType) {
            case INSERT:
                operationType = DataOperaType.INSERT;
                break;
            case UPDATE:
                operationType = DataOperaType.UPDATE;
                break;
            case DELETE:
                operationType = DataOperaType.DELETE;
                break;
            case ROW_CHAIN:
                if (CollectionUtils.isEmpty(beforeColumnsList)) {
                    operationType = DataOperaType.INSERT;
                    break;
                }
                operationType = DataOperaType.UPDATE;
                break;
            case TRUNCATE:
                operationType = DataOperaType.TRUNCATE;
                break;
            default:
                operationType = DataOperaType.OTHERS;
                break;
            // 其余类型不处理
        }

        if (DSyncEntry.Type.ORACLE.equals(entry.getHeader().getSourceType())) {
            ValueColumn lsnColumn = new ValueColumn(IncrementConstant.DB_LSN, Type.stringType());
            //mysql需要维护两个值：entry.getHeader().getLogfileOffset();
            String lsn = entry.getHeader().getLastChangeNumber();
            lsnColumn.setData(new Value(lsn));
            resultList.add(lsnColumn);

            ValueColumn rowidColumn = new ValueColumn(IncrementConstant.DB_ROWID, Type.stringType());
            rowidColumn.setData(new Value(rowData.getRowId()));
            resultList.add(rowidColumn);
        }
        if (DSyncEntry.Type.SQLSERVER.equals(entry.getHeader().getSourceType())) {
            //sqlserver 挖掘伪列都是小写，需要转大写
            ValueColumn rowidColumn = new ValueColumn(IncrementConstant.DB_ROWID, Type.stringType());
            rowidColumn.setData(new Value(rowData.getRowId().toUpperCase()));
            resultList.add(rowidColumn);
        }

        ValueColumn operationTypeColumn = new ValueColumn(IncrementConstant.OPERATION, Type.stringType());
        operationTypeColumn.setData(new Value(operationType));
        resultList.add(operationTypeColumn);

        ValueColumn executeTimeColumn = new ValueColumn(IncrementConstant.EXECUTE_TIME, Type.longType());
        Long executeTime = entry.getHeader().getExecuteTime();
        executeTimeColumn.setData(new Value(executeTime));
        resultList.add(executeTimeColumn);

        ValueColumn schema = new ValueColumn(IncrementConstant.SCHEMA, Type.stringType());
        String schemaName = entry.getHeader().getSchemaName();
        schema.setData(new Value(schemaName));
        resultList.add(schema);

        ValueColumn table = new ValueColumn(IncrementConstant.TABLE, Type.longType());
        String tableName = entry.getHeader().getObjectName();
        table.setData(new Value(tableName));
        resultList.add(table);

        return resultList;
    }


    private final static List<Integer> HEX_LIST =
            Arrays.asList(BinaryColumnConstant.ORACLE_TYPE_RAW, BinaryColumnConstant.ORACLE_TYPE_LONG_RAW);

    private final static List<Integer> BINARY_LIST = Arrays
            .asList(BinaryColumnConstant.BINARY, BinaryColumnConstant.VARBINARY, BinaryColumnConstant.LONGVARBINARY,
                    BinaryColumnConstant.BLOB, BinaryColumnConstant.TIMESTAMP, BinaryColumnConstant.IMAGE);
    /**
     * 将值进行一定的进制转换
     *
     * @param column
     * @return java.lang.Object
     * @author mayongjie
     * @date 2022/03/23 19:50
     */
    private static Object getColumnValue(DSyncEntry.Column column) {
        if (column == null || column.getIsNull()) {
            return null;
        }
        if (HEX_LIST.contains(column.getSqlType())) {
            try {
                return bytes2String(column.getValue().getBytes("ISO_8859_1"));
            } catch (UnsupportedEncodingException e) {
                return column.getValue();
            }
        }
        if (BINARY_LIST.contains(column.getSqlType())) {
            try {
                return column.getValue().getBytes("ISO_8859_1");
            } catch (UnsupportedEncodingException e) {
                return column.getValue();
            }
        }
        return column.getValue();
    }


    /**
     * byte数组的十六进制显示
     *
     * @param values
     * @return
     */
    public static String bytes2String(byte[] values) {
        if (values == null) {
            return null;
        }
        StringBuilder valueBuilder = new StringBuilder();
        for (byte b : values) {
            valueBuilder.append(Integer.toHexString(b & 0xFF).toUpperCase());
        }
        return valueBuilder.toString();
    }
}
