package com.chail.datasupport.tools.model;

import com.chail.Db2ObjectFiled;
import lombok.Data;

/**
 * @author : yangc
 * @date :2022/5/12 21:26
 * @description :
 * @modyified By:
 */
@Data
public class Job {
    @Db2ObjectFiled("id")
    private String id;
    @Db2ObjectFiled("job_id")
    private String jobId;
    @Db2ObjectFiled("name")
    private String jobName;

    @Db2ObjectFiled("extid")
    private String extId;

    @Db2ObjectFiled("status")
    private String status;



    /**
     * @author : yangc
     * @date :2022/5/31 16:50
     * @description :
     * @modyified By:
     */
    @Data
    public static class JobTask {

        @Db2ObjectFiled("id")
        private String id;

        @Db2ObjectFiled("name")
        private String name;

        @Db2ObjectFiled("source_schema_name")
        private String  sourceSchema;

        @Db2ObjectFiled("source_table_name")
        private String sourceTableName;

        @Db2ObjectFiled("target_schema_name")
        private String targetSchema;

        @Db2ObjectFiled("target_table_name")
        private String targetTableName;

        @Db2ObjectFiled("diff_h")
        private String  diffHour;

        @Db2ObjectFiled("diff_d")
        private String diffDay;

        @Db2ObjectFiled("last_modify_time")
        private String lastModifyTime;

        @Db2ObjectFiled("job_id")
        private String jobId;

        @Db2ObjectFiled("job_history_id")
        private String jobHisId;

        @Db2ObjectFiled("tq_success_count")
        private String tqSucess;

        @Db2ObjectFiled("task_status")
        private String taskStatus;

        @Db2ObjectFiled("count")
        private String count;

        @Db2ObjectFiled("sql")
        private String sql;

        @Db2ObjectFiled("table_id")
        private String tableId;

        @Db2ObjectFiled("value")
        private String exJobid;

        @Db2ObjectFiled("job_name")
        private String jobName;

        @Db2ObjectFiled("task_id")
        private String taskId;

        @Db2ObjectFiled("desc")
        private String desc;


        public String getTSchemaAndTableName(){
            return targetSchema+"."+targetTableName;
        }

        public String getSSchemaAndTableName(){
            return sourceSchema+"."+sourceTableName;
        }

    }
}
