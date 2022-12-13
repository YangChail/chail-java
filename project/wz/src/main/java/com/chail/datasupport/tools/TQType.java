package com.chail.datasupport.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.chail.datasupport.tools.model.Job;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author : yangc
 * @date :2022/6/13 16:21
 * @description :
 * @modyified By:
 */
public class TQType extends CheckJob{
    public static void main(String[] args) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        ///243/605/246/247/577/247/636
        getConnection();
        tqType("247","636");
        //getsql("245","590");

    }


    /**
     *
     *
     * INSERT INTO mc_job_table_config (job_id,table_id,"name","value") VALUES (225,42542,'EXTRACT_STRATEGY','BOUNDED_PAGINATION')
     * INSERT INTO mc_job_table_config (job_id,table_id,"name",value) VALUES (225,42542,'BY_AGENT','true');
     *
     *
     * TABLE_SCAN
     * EXTRACT_STRATEGY
     *
     *
     * @param jobid
     * @param jobHisId
     * @throws SQLException
     * @throws IntrospectionException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public static void tqType(String jobid,String jobHisId) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String sql = "select * from mc_job_task mjt where job_id =%s and job_history_id =%s  and task_status not in ('SYNCHRONIZING')";
        DruidPooledConnection connection = dataSource.getConnection();
        sql=String.format(sql,jobid,jobHisId);
        ResultSet rs = getResult(connection,sql);
        List<Job.JobTask> list=new ArrayList<>();
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            list.add(jobTask);
        }

        String[] node=new String[]{"engine77","engine78","engine81"};


        Random random=new Random();
        for (Job.JobTask jobTask : list) {
            int i = random.nextInt(3);

            String sql1="INSERT INTO mc_job_table_config (job_id,table_id,\"name\",\"value\") VALUES (%s,%s,'EXTRACT_STRATEGY','BOUNDED_PAGINATION');";
            String sql2="INSERT INTO mc_job_table_config (job_id,table_id,\"name\",value) VALUES (%s,%s,'BY_AGENT','true');";
            String sql3="INSERT INTO mc_job_table_config (job_id,table_id,\"name\",value) VALUES (%s,%s,'FULL_PAGE_SIZE','100000');";
            String sql4="INSERT INTO mc_job_table_config (job_id,table_id,\"name\",value) VALUES (%s,%s,'ENGINE_NODE_LABLE','%s');";
            System.out.println(String.format(sql1,jobid,jobTask.getTableId()));
            System.out.println(String.format(sql2,jobid,jobTask.getTableId()));
            System.out.println(String.format(sql3,jobid,jobTask.getTableId()));
            System.out.println(String.format(sql4,jobid,jobTask.getTableId(),node[i]));
        }
    }


}
