package com.chail.datasupport.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.chail.datasupport.tools.model.Job;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : yangc
 * @date :2022/6/10 13:51
 * @description :
 * @modyified By:
 */
public class CreateTable extends CheckJob {


    public static void main(String[] args) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        //247/575/247/576//247/577
        createTable("247","577");
    }

    public static void createTable(String jobId,String hisJobId) throws SQLException, IntrospectionException, InvocationTargetException,
            IllegalAccessException {
        String jobSql = "select * from mc_db_object_sql mdos right join mc_job_task mjt on mdos.table_name = mjt.source_table_name and mdos.schema_name = mjt.source_schema_name where source_id in( select mss.source_id from mc_job job, mc_sensitive_source mss where job.input_id = mss.id and job.name like '苍南人民医院%%' ) and mdos.object_type = 'TABLE' and mjt.job_id =%s and mjt.job_history_id =%s";
        jobSql=String.format(jobSql,jobId,hisJobId);
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection,jobSql);
        List<Job.JobTask> list = new ArrayList<>();
        String add = ", \n" +
                "\"ods_insert_time\" DATE DEFAULT sysdate, \n" +
                "\"ods_update_time\" TIMESTAMP (6), \n" +
                "\"ods_delete_flag\" CHAR(1), \n" +
                "\"source_change_time\" TIMESTAMP (6), \n" +
                "\"ods_oper_type\" CHAR(1));";
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            String sql = jobTask.getSql().replace("${TABLE_NAME}", jobTask.getTargetSchema() + "." + jobTask.getTargetTableName());
            sql = sql.replace("in ${MC_TABLE_SPACE_NAME}", "");
            sql = sql.replace(");", add);

            //String drop=String.format("drop table %s.%s;",jobTask.getTargetSchema(),jobTask.getTargetTableName());
            sql = "\n" + sql;
            jobTask.setSql(sql);
            list.add(jobTask);
        }
        for (Job.JobTask jobTask : list) {
            System.out.println(jobTask.getSql());
        }

    }
}
