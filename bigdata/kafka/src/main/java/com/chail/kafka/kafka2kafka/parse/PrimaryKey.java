package com.chail.kafka.kafka2kafka.parse;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * 主键
 * 各类主键列表，业务根据需要的顺序自取
 * Ex：增量的DML获取顺序：hash > logic > primary > unique
 *
 * @author mayongjie
 * @date 2022/04/12 10:11
 **/
@Getter
@Setter
public class PrimaryKey {

    /**
     * 物理主键列表
     */
    private List<Column> primaryList;

    /**
     * 唯一键列表
     */
    private List<Column> uniqueList;

    /**
     * 逻辑主键列表
     */
    private List<Column> logicList;

    /**
     * hash主键列表
     */
    private List<Column> hashList;

}
