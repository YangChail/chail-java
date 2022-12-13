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
 * @date :2022/6/8 10:55
 * @description :
 * @modyified By:
 */
public class Agent extends CheckJob{

    public static void main(String[] args) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        ///265/563//259/541//254/534/245/573237/532/253/509//245/573//253/509//265/563//246/517//254/534//233/488249/513/253/509/
        ///281/571//247/636/
        getConnection();
        getsql("247","636");
    }


    public static void  getsql(String jobid,String jobHisId) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        DruidPooledConnection connection = dataSource.getConnection();
        String sql="select\n" +
                "'update xxl_job_info set job_group =2, executor_route_strategy=''CONSISTENT_HASH'' where id='|| xji.id ||';' as sql,\n" +
                "*\n" +
                "from\n" +
                "xxl_job_info xji\n" +
                "left join xxl_job_group xjg on\n" +
                "xji.job_group = xjg.id\n" +
                "where\n" +
                "xjg.app_name in (\n" +
                "select\n" +
                "conf_value\n" +
                "from\n" +
                "mc_source_config msc\n" +
                "where\n" +
                "conf_code = 'engine_node'\n" +
                "and source_id in (\n" +
                "select\n" +
                "sor.source_id\n" +
                "from\n" +
                "mc_job job\n" +
                "left join mc_sensitive_source sor on\n" +
                "sor.id = job.input_id\n" +
                "where\n" +
                "job.id = %s ))\n" +
                "and executor_handler = 'fullSyncHandler'\n" +
                "and job_desc in (\n" +
                "select\n" +
                "mjt.source_schema_name || '.' || mjt.source_table_name\n" +
                "from\n" +
                "mc_job_task mjt\n" +
                "where\n" +
                "job_id = %s\n" +
                "and job_history_id = %s\n" +
                "and mjt.task_status not in ('SYNCHRONIZING') );\n";
        String sql2="select\n" +
                "'insert into mc_job_config (job_id,\"name\",\"value\") values (' || mjt.job_id || ',''AGENT_TABLE_' || mjt.source_schema_name || '.' || mjt.source_table_name || ''',true) ;' as sql ,\n" +
                "mjt.*\n" +
                "from\n" +
                "mc_job_task mjt\n" +
                "where\n" +
                "job_id = %s\n" +
                "and job_history_id = %s\n" +
                "and mjt.task_status not in ('SYNCHRONIZING')\n";
        sql= String.format(sql,jobid,jobid,jobHisId);
        sql2= String.format(sql2,jobid,jobHisId);
        ResultSet rs = getResult(connection,sql);
        List<Job.JobTask> list = new ArrayList<>();
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            list.add(jobTask);
        }

        rs = getResult(connection,sql2);
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            list.add(jobTask);
        }

        for (Job.JobTask jobTask : list) {
            System.out.println(jobTask.getSql());
        }


    }






}
