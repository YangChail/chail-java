package org.chail.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class JobTask {

    private Long id;

    /**
     * 作业task名
     */
    private String name;

    /**
     * 父任务ID
     */
    private Long parentTaskId;

    /**
     * 源schema
     */
    private String sourceSchemaName;

    /**
     * 源表名
     */
    private String sourceTableName;

    /**
     * 目标schema
     */
    private String targetSchemaName;

    /**
     * 目标表名
     */
    private String targetTableName;

    /**
     * 作业ID
     */
    private Long jobId;

    /**
     * 历史作业ID
     */
    private Long jobHistoryId;

    /**
     * 全量成功数
     */
    private Long tqSuccessCount;

    /**
     * 全量失败数
     */
    private Long tqFailureCount;

    /**
     * 增量成功数
     */
    private Long incSuccessCount;

    /**
     * 增量失败数
     */
    private Long incFailureCount;

    private String taskStatus;

    /**
     * 期望状态
     */
    private String taskExpectStatus;

    private String taskStep;

    /**
     * 开始时间
     */
    private Date startTime;

    /**
     * 最新修改时间
     */
    private Date lastModifyTime;

    /**
     * 预留字段
     */
    private String advanceOption;

    /**
     * 全量结束时间
     */
    private Date tqEndTime;

    /**
     * 全量开始时间
     */
    private Date tqStartTime;

    private Integer tableId;


    /**
     * 表格创建状态
     */
    private String tableCreate;

    /**
     * 下次允许调度时间
     */
    private Date nextFireTime;

    private Long kafkaOffset;

    /**
     * 失败的调度次数
     */
    private int failTimes;

    /**
     * 是否清理表数据
     */
    private Boolean truncateEnable;

    /**
     * 是否构建中
     */
    private Boolean building;

    private static final long serialVersionUID = 1L;

    /**
     * 子task为分区维度，父task为表维度
     *
     * @return
     */
    public String getSourceSchemaTableName() {
        return this.sourceSchemaName + "." + this.sourceTableName;
    }

    public String getTargetSchemaTableName() {
        return this.targetSchemaName + "." + this.targetTableName;
    }
}
