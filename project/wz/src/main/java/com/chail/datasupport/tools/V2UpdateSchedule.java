package com.chail.datasupport.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.chail.HttpClientUtil;
import com.chail.datasupport.tools.model.*;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author : yangc
 * @date :2022/6/28 15:39
 * @description :
 * @modyified By:
 */
public class V2UpdateSchedule extends Ds2{

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException {

        getConnection();
        List<Job> allJob = getAllJob();

        //节点
        //setJobConfig(allJob);


        //setJobScheduleHttp(allJob);
        deleteSchedule(allJob);



    }

    private static void setJobConfig( List<Job> allJob) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        for (Job job : allJob) {
            String jobId = job.getId();
            if(!job.getJobName().contains("检验")){
                continue;
            }
            JobConfig config = getConfig(jobId);

            List<SqlObject> sqlObjects = config.getSqlObjects();
            for (SqlObject sqlObject : sqlObjects) {
                List<Integer> sensitiveIds = sqlObject.getSensitiveIds();
                for (Integer sensitiveId : sensitiveIds) {
                    List<Table> tables = getTables(sensitiveId + "");
                    for (Table table : tables) {
                        DruidPooledConnection connection = dataSource.getConnection();
                        String id = table.getId();
                        String check="select * from mc_job_table_config where job_id =%s and table_id =%s and name='ENGINE_NODE_LABLE'";
                        check = String.format(check,jobId,id);
                        ResultSet rs = getResult(connection,check);
                        if(rs.next()){
                            System.out.println("----------------");
                            CloseUtils.close(connection,rs);
                            continue;
                        }
                        String sql="insert into mc_job_table_config (job_id,table_id,\"name\",value) values(%s,%s,'ENGINE_NODE_LABLE','195engine');";
                        sql = String.format(sql, jobId,id);
                        //System.out.println(table.getDes());
                        System.out.println(sql);
                        CloseUtils.close(connection,rs);
                    }
                }
            }
        }
    }



    public static List<Table> getTables(String sensitiveId) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String sql ="select * from mc_sensitive_table mst where sensitive_source_id =%s";
        sql = String.format(sql, sensitiveId);
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection,sql);
        List<Table> list = new ArrayList<>();
        while (rs.next()) {
            Table table = new Table();
            Db2ObjectUtils.getObj(rs, table, Table.class);
            list.add(table);
        }
        CloseUtils.close(connection,rs);
        return list;
    }



    public static  List<Job>  getAllJob() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql = 	"select\n" +
                "\tjob.id,\n" +
                "\tjob.name,\n" +
                "\tmjc .value as extid\n" +
                "\n" +
                "from\n" +
                "\tmc_job job\n" +
                "left join mc_job_config mjc on\n" +
                "\tjob .id = mjc.job_id\n" +
                "where\n" +
                "\tmjc.\"name\" = 'JOB_EXTERNAL_ID'";
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection,jobSql);
        List<Job> list = new ArrayList<>();
        while (rs.next()) {
            Job job = new Job();
            Db2ObjectUtils.getObj(rs, job, Job.class);
            list.add(job);
        }
        CloseUtils.close(connection,rs);
        return list;
    }




    public static JobConfig getConfig(String jobId) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String sql = "select * from mc_job_config  where job_id =%s and name = 'MODEL_SOURCE_ID_DEPEND'";
        sql = String.format(sql, jobId);
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection,sql);
        JobConfig jobConfig = new JobConfig();
        while (rs.next()) {
            Db2ObjectUtils.getObj(rs, jobConfig, JobConfig.class);
        }
        CloseUtils.close(rs, rs.getStatement(),connection);
        String value = jobConfig.getValue();
        List<SqlObject> sqlObjects = JSONObject.parseArray(value, SqlObject.class);
        jobConfig.setSqlObjects(sqlObjects);
        return jobConfig;
    }




    private static String deleteSchedule(List<Job> allJob){
        List<TaskEditView> reslist=new ArrayList<>();
        for (Job job : allJob) {
            String extId = job.getExtId();
            TaskEditView taskEditView=new TaskEditView();
            taskEditView.setJobId(Integer.valueOf(extId));
            taskEditView.setSchedulingStatus("true");
            taskEditView.setScheduleCorn("");
            taskEditView.setScheduleCycleType("custom");
            taskEditView.setScheduleCycle("rightaway");
            taskEditView.setType(TaskEditView.Type.SCHEDULE);
            reslist.add(taskEditView);
        }

        int i=0;
        List<TaskEditView> list=new ArrayList<>();
        for (TaskEditView taskEditView : reslist) {
            list.add(taskEditView);
            i++;
            if(i%10==0){
                JSONArray array= JSONArray.parseArray(JSON.toJSONString(list));
                String s = array.toString();
                String url="http://wz.chail.top:61043/api/v2/job/sync/table/edit";
                String s1 = HttpClientUtil.doPost(url, null, null, s);
                System.out.println(s1);
                list.clear();
                i=0;
            }
        }
        return "";
    }


    private static String setJobScheduleHttp(List<Job> allJob){
        String corn3="0 0/3 * * * ? *";
        List<Integer> min=new ArrayList<>();
        for(int i=30;i<60;i++){
            min.add(i);
        }
        List<TaskEditView> reslist=new ArrayList<>();
        for (Job job : allJob) {
            String corn="";
            if(job.getJobName().contains("检验")){
                corn=corn3;
            }else{
                Collections.shuffle(min);
                Integer integer = min.get(0);
                corn=String.format("0 %s * * * ? *",integer);
            }
            String extId = job.getExtId();
            TaskEditView taskEditView=new TaskEditView();
            taskEditView.setJobId(Integer.valueOf(extId));
            taskEditView.setSchedulingStatus("true");
            taskEditView.setScheduleCorn(corn);
            taskEditView.setScheduleCycleType("custom");
            taskEditView.setScheduleCycle("timing");
            taskEditView.setType(TaskEditView.Type.SCHEDULE);
            reslist.add(taskEditView);
        }

        for (TaskEditView taskEditView : reslist) {
            List<TaskEditView> list=new ArrayList<>();
            list.add(taskEditView);
            JSONArray array= JSONArray.parseArray(JSON.toJSONString(list));
            String s = array.toString();
            String url="http://wz.chail.top:61043/api/v2/job/sync/table/edit";
            String s1 = HttpClientUtil.doPost(url, null, null, s);
            System.out.println(s1);
        }
        return "";
    }

}
