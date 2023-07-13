package com.chail.kafka.kafka2kafka.parse;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * 字段
 * @author mayongjie
 * @date 2022/03/17 17:01
 **/
@Getter
@Setter
@ToString
public class Column implements Cloneable, Serializable {

    /**
     * 字段名
     */
    private String name;

    /**
     * 字段类型
     */
    private Type type;

    /**
     * 字段扩展
     * fx: 脱敏、清洗
     */

    public Column(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public Column clone() {
        try {
            Column clone = (Column) super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
