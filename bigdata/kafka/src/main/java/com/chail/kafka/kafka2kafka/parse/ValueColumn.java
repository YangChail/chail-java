package com.chail.kafka.kafka2kafka.parse;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 含值字段
 *
 * @author mayongjie
 * @date 2022/03/23 14:06
 **/
@Getter
@Setter
@ToString
public class ValueColumn extends Column{

    public ValueColumn(String name, Type type) {
        super(name, type);
    }

    public ValueColumn(String name, Type type, Value data, Value beforeData) {
        super(name, type);
        this.data = data;
        this.beforeData = beforeData;
    }

    public ValueColumn(String name, Type type, Value data, Value beforeData, boolean needUpdate) {
        super(name, type);
        this.data = data;
        this.beforeData = beforeData;
    }

    /**
     * 数据内容
     */
    private Value data;

    /**
     * 数据前值
     * 增量
     */
    private Value beforeData;

    /**
     * 是否需要更新
     */
}
