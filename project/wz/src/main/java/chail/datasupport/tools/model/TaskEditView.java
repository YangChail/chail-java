package chail.datasupport.tools.model;

import lombok.Data;
import lombok.Getter;

@Data
public class TaskEditView {
    /**
     * jobId
     */
    private Integer jobId;

    private IncSetView incset;

    /**
     * 修改后的jobName
     */
    private String jobName;

    /**
     * 原jobName所属schema
     */
    private String schemaName;

    /**
     * 原tableName所属schema
     */
    private String tableName;
    /**
     * 任务调度开启或关闭
     */
    private String schedulingStatus;
    /**
     * 调度执行立即执行或定时执行
     */
    private String scheduleCycle;
    /**
     * 关联关系编辑(Sql)
     */
    private String sql;

    private String incSql;

    /**
     * 执行周期week,day,month
     */
    private String scheduleCycleType;
    /**
     * 启动时间
     */
    private String startTime;
    /**
     * 调度日期
     */
    private String startDate;
    /**
     * 结束日期
     */
    private String endDate;
    /**
     * 调度表达式
     */
    private String scheduleCorn;

    /**
     * 表格配置映射名称
     */
    private String mappingName;

    /**
     * 修改类型
     */
    private Type type;

    @Getter
    public enum Type{
        /**
         * sql代表修改目标映射, name代表修改名称，schedule代表修改调度，mappingName代表修改映射名称
         */
        SQL("sql"),NAME("name"),SCHEDULE("schedule"),MAPPINGNAME("mappingName");

        private final String type;

        Type(String type) {
            this.type = type;
        }
    }
}