package com.chail.kafka.kafka2kafka.parse;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaFactory;

/**
 * 类型
 * @author mayongjie
 * @date 2022/03/17 17:01
 **/
@Getter
@Slf4j
@ToString
public class Type {

    private ValueMetaInterface valueMeta;

    /**
     * 数据库源类型
     */
    private String sourceType;

    /**
     * 长度
     */
    private int length;

    /**
     * 精度
     */
    private int scale;

    public Type(ValueMetaInterface valueMeta) {
        this.valueMeta = valueMeta;
    }

    public Type(ValueMetaInterface valueMeta, String sourceType, int length, int scale) {
        this.valueMeta = valueMeta;
        this.sourceType = sourceType;
        this.length = length;
        this.scale = scale;
    }

    public static Type stringType() {
        return getType(ValueMetaBase.TYPE_STRING);
    }

    public static Type longType() {
        return getType(ValueMetaBase.TYPE_BIGNUMBER);
    }

    public static Type dateType() {
        return getType(ValueMetaBase.TYPE_DATE);
    }

    private static Type getType(int type) {
        return new Type(null);
    }


}
