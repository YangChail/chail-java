package com.chail.kafka.kafka2kafka.parse;

/**
 * Description:
 * datetime: 2019/9/11 16:20
 * @author: ningyu
 */
public enum DataOperaType {
    /**
     * 插入
     */
    INSERT,
    /**
     * 更新
     */
    UPDATE,
    /**
     * 删除
     */
    DELETE,
    /**
     * 清空
     */
    TRUNCATE,
    /**
     * DDL变动
     */
    DDL,
    /**
     * 其他
     */
    OTHERS,
    HIVE_INSERT,
    ;
}
