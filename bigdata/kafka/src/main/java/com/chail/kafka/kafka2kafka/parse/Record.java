package com.chail.kafka.kafka2kafka.parse;

import com.mchz.mcdatasource.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.Serializable;
import java.util.Map;

/**
 * 记录
 *
 * @author mayongjie
 * @date 2022/03/17 16:57
 **/
public abstract class Record implements Serializable {

    /**
     * 标记字段顺序
     */
    @Getter
    @Setter
    public Map<String, ValueColumn> columnMap;



    private long sizeInBytes = 0L;

    /**
     * 前置操作删除数据的时候，发现目标有删除记录，就标记为update操作
     */
    private boolean updateOpearflag;

    /**
     * 当前记录的分区位置，用于更新位点
     */
    @Getter
    @Setter
    private int partitionIndex;


    /**
     * 添加数据
     *
     * @param column
     * @author mayongjie
     * @date 2022/03/17 17:08
     */
    public abstract void addColumn(ValueColumn column);

    /**
     * 获取数据
     *
     * @param name 字段名
     * @return com.mchz.runner.model.column.Column
     * @author mayongjie
     * @date 2022/03/17 18:07
     */
    public abstract ValueColumn getColumn(String name);

    /**
     * 移除数据
     *
     * @param name
     * @return com.mchz.plugin.model.column.ValueColumn 返回被移除的数据
     * @author mayongjie
     * @date 2022/11/09 10:54
     */
    public abstract ValueColumn removeColumn(String name);


    public abstract Object[] getValues();

    public Object getValueForValueColumn(String name) {
        ValueColumn column = getColumn(name);
        if (column == null) {
            return null;
        }
        return column.getData().getValue();
    }

    public boolean isUpdateOpearflag() {
        return updateOpearflag;
    }

    public void setUpdateOpearflag(boolean updateOpearflag) {
        this.updateOpearflag = updateOpearflag;
    }


    public ValueColumn getValue(String column) {
        return getColumnMap().get(column);
    }

    public void putValue(String column, ValueColumn value) {
        if (StringUtils.isNotBlank(column)) {
            getColumnMap().put(column, value);
        }
    }

    public Object removeValue(String column) {
        return getColumnMap().remove(column);
    }

    public Map<String, ValueColumn> getColumnMap() {
        return columnMap;
    }

    public boolean containsColumn(String column) {
        return columnMap.containsKey(column);
    }

    public ValueMetaInterface getMeta(String columnName) {
        return columnMap.get(columnName).getType().getValueMeta();
    }


    public long getSizeInBytes() {
        return sizeInBytes;
    }

}
