package com.chail.kafka.kafka2kafka.parse;

/**
 * 增量常量
 *
 * @author mayongjie
 * @date 2022/03/23 17:22
 **/
public class IncrementConstant {

    /**
     * 操作类型字段名
     */
    public static final String OPERATION = "#_mc_operation";

    /**
     * 执行时间
     */
    public static final String EXECUTE_TIME = "#_mc_execute_time";

    /**
     * ddl sql
     */
    public static final String DDL = "#_mc_ddl_sql";

    /**
     * 提交消费者
     */
    public static final String CONSUMER_KEY = "#_mc_commit_consumer";

    /**
     * 批量提交量
     */
    public static final String BATCH_SIZE = "#_mc_batch_size";

    /**
     * 消息顺序号
     */
    public static final String SEQUENCE = "#_mc_sequence";

    /**
     * 解析分页配置
     */
    public static final String PAGE_SIZE = "pageSize";
    public static final String PAGE = "page";

    /**
     * schema信息
     */
    public static final String SCHEMA = "#_mc_schema";

    /**
     * 表名信息
     */
    public static final String TABLE = "#_mc_table";

    /**
     * 任务唯一标识
     */
    public static final String ID = "#_mc_jobIdAndTaskId";

    /**
     * 冗余时间
     */
    public static final String REDUNDANT_TIME = "#_mc_redundant_time";

    /**
     * 源数据库的字符集
     */
    public static final String SOURCE_CHARSET = "SOURCE_CHARSET";

    /**
     * 操作系统的字符集
     */
    public static final String SYSTEM_CHARSET = "SYSTEM_CHARSET";

    /**
     * 目标数据库字符集
     */
    public static final String TARGET_CHARSET = "TARGET_CHARSET";

    /**
     * 数据库 lsn 标识
     **/
    public static final String DB_LSN = "#_mc_db_lsn";
    public static final String DB_ROWID = "#_mc_db_rowid";
}
