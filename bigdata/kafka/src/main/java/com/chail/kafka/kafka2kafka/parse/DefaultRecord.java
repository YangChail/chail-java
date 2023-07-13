package com.chail.kafka.kafka2kafka.parse;

import com.google.common.collect.Maps;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;


/**
 * @author mayongjie
 * @date 2022/03/23 14:12
 **/
@ToString
@Slf4j
public class DefaultRecord extends Record {

    public DefaultRecord() {
        this.columnMap = new HashMap<>();
    }

    public DefaultRecord(Map<String, ValueColumn> columnMap) {
        this.columnMap = columnMap;
    }


    @Override
    public void addColumn(ValueColumn column) {
        ValueColumn existColumn = null;
        try {
            existColumn = getColumn(column.getName());
        } catch (Exception ignored) {
            // 忽略获取不到的异常
        }
        if (!(existColumn instanceof ValueColumn)) {
            columnMap.put(column.getName(), column);
        } else {
            existColumn.setData(column.getData());
            existColumn.setBeforeData(column.getBeforeData());
            columnMap.put(column.getName(), existColumn);
        }
    }

    @Override
    public ValueColumn getColumn(String name) {
        ValueColumn valueColumn = columnMap.get(name);
        if (valueColumn != null) {
            return valueColumn;
        }
        return new ValueColumn(name, null, new Value(null, false), new Value(null, false), false);

    }

    @Override
    public ValueColumn removeColumn(String name) {
        return columnMap.remove(name);
    }

    @Override
    public Record clone() {
        return new DefaultRecord(Maps.newHashMap(this.columnMap));
    }

    @Override
    public Object[] getValues() {
        return columnMap.values().toArray();
    }
}
