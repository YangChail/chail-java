package com.chail.kafka.kafka2kafka.parse;

import lombok.ToString;

import java.io.Serializable;

/**
 * 值封装
 *
 * @author mayongjie
 * @date 2022/04/12 09:20
 **/
@ToString
public class Value implements Serializable {

    /**
     * 值
     */
    private Object value;

    /**
     * 是否为空
     * 由于null是关键字，所以使用isNull作为变量名
     */
    private Boolean hasNull;

    public Value(Object value) {
        this(value, false);
    }

    public Value(Object value, Boolean hasNull) {
        this.value = value;
        this.hasNull = hasNull;
    }

    public Object getValue() {
        if (hasNull) {
            return null;
        }
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Boolean isNull() {
        return hasNull;
    }

    public Boolean getHasNull() {
        return hasNull;
    }

    public void setHasNull(Boolean hasNull) {
        this.hasNull = hasNull;
    }
}
