package chail.datasupport.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import chail.HttpClientUtil;
import com.chail.datasupport.tools.model.Job;
import com.chail.datasupport.tools.model.RestartTableDto;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author : yangc
 * @date :2022/6/22 18:38
 * @description :
 * @modyified By:
 */
public class V2JobService extends Ds2 {


    //泰顺人民医院
    public static void main(String[] args) {

        try {
            getConnection();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }


//        while (true){
//            //autoRunJob();
//            try {
//                failureTableRestart();
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//            try {
//                Thread.sleep(200000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

        while (true){
            try {
                updateJobStatus();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
               Thread.sleep(60*60*1000);
           } catch (InterruptedException e) {
                e.printStackTrace();
           }
        }


    }


    public static void updateJobStatus() throws Exception {
        String sql = "select * from mc_job_history_new mjhn where status ='RUN_FAILURE'";
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, sql);
        List<Job> list = new ArrayList<>();
        while (rs.next()) {
            Job job = new Job();
            Db2ObjectUtils.getObj(rs, job, Job.class);
            list.add(job);
        }
        CloseUtils.close(connection, rs);
        List<Job.JobTask> list1 = new ArrayList<>();
        for (Job job : list) {
            //sql=String.format("select * from mc_job_task where job_id =%s and job_history_id =%s and task_status= 'FAILURE'",job.getJobId(),job.getId());
            sql = String.format("select task.*,mst.description as desc from mc_job_task task,mc_sensitive_table mst  where  mst.id =task.table_id  and job_id =%s and job_history_id =%s and task_status= 'FAILURE'", job.getJobId(), job.getId());
            connection = dataSource.getConnection();
            rs = getResult(connection, sql);
            while (rs.next()) {
                Job.JobTask jobTask = new Job.JobTask();
                Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
                list1.add(jobTask);
            }
            CloseUtils.close(connection, rs);
        }

        int num = 0;
        for (Job.JobTask jobTask : list1) {
            String name = jobTask.getDesc();
            List<Long> ids = new ArrayList<>();
            ids.add(Long.valueOf(jobTask.getId()));
            RestartTableDto restartTableDto = new RestartTableDto();
            restartTableDto.setIds(ids);
            restartTableDto.setFromTotal(true);
            System.out.println(name);
            num++;
            System.out.println(restartTableHttp(restartTableDto));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

//            if (num > 40) {
//                break;
//            }
            //统计
        }
        System.out.println(num);

        for (Job job : list) {
            sql=String.format("select * from mc_job_task where job_id =%s and job_history_id =%s and task_status= 'FAILURE'",job.getJobId(),job.getId());
            connection = dataSource.getConnection();
            rs = getResult(connection,sql);
            if(!rs.next()){
                sql=String.format("update mc_job_history_new set status = 'RUNNING' where id=%s",job.getId());
                connection.getConnection().createStatement().execute(sql);
                System.out.println("更新成功");
            }
            CloseUtils.close(connection,rs);
        }
    }

    /**
     * 失败表格重启
     *
     * @throws SQLException
     * @throws IntrospectionException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public static void failureTableRestart() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {

        //正在运行的表格
        List<Job.JobTask> runTask = getRunTask();
        if (runTask.size() > 20) {
            log("正在运行的表格大于15，实际：" + runTask.size());
            //return;
        }
        //and last_modify_time < '2022-06-24 18:24:28.347'
        String jobSql = "select * from mc_job_task mjt where task_status ='FAILURE' and last_modify_time < '2022-07-25 23:30:28.347' and task_expect_status !='MANUAL_STOP'   order by tq_success_count ";
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, jobSql);
        List<Job.JobTask> list = new ArrayList<>();
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            list.add(jobTask);
        }
        CloseUtils.close(connection, rs);

        List<Long> restartList = new ArrayList<>();
        for (Job.JobTask jobTask : list) {
            List<Long> ids = new ArrayList<>();
            ids.add(Long.valueOf(jobTask.getId()));
            RestartTableDto restartTableDto = new RestartTableDto();
            restartTableDto.setIds(ids);
            restartTableDto.setFromTotal(true);
            log("发送http。。。。。。。。。" + ids.toString());
            System.out.println(restartTableHttp(restartTableDto));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //统计
            restartList.add(Long.valueOf(jobTask.getId()));
            if (runTask.size() + restartList.size() > 20) {
                break;
            }
        }

//        String s = countStatus();
//        //发微信
//        StringBuffer stringBuffer=new StringBuffer("自动全量运行-失败表格-开始运行:\n").append("taskIds:")
//                .append(restartList).append("\n").append("===目标模型情况===").append("\n").append(s);
//        HttpClientUtil.send(stringBuffer.toString());
    }


    private static String countStatus() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql = "select count(*) as count,task_status from mc_job_task  group by task_status order by task_status ";
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, jobSql);
        List<Job.JobTask> list = new ArrayList<>();
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            list.add(jobTask);
        }
        CloseUtils.close(connection, rs);
        StringBuffer sbf = new StringBuffer();
        for (Job.JobTask jobTask : list) {
            if ("SUCCESS".equals(jobTask.getTaskStatus())) {
                sbf.append("完成:").append(jobTask.getCount()).append("\n");
            } else if ("SYNCHRONIZING".equals(jobTask.getTaskStatus())) {
                sbf.append("运行中:").append(jobTask.getCount()).append("\n");
            } else if ("FAILURE".equals(jobTask.getTaskStatus())) {
                sbf.append("异常停止:").append(jobTask.getCount()).append("\n");
            }
        }
        return sbf.toString();
    }

    private static String restartTableHttp(RestartTableDto restartTableDto) {

        try {
            String url = "http://wz.chail.top:61043/api/v1/monitor/job/table/restart";
            String s = JSONObject.toJSONString(restartTableDto);
            return HttpClientUtil.doPost(url, null, null, s);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";

    }


    public static void autoRunJob() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job> allJob = getAllJob();
        //所有的作业
        allJob = allJob.stream().sorted(Comparator.comparing(Job::getJobName)).collect(Collectors.toList());
        //完成的作业
        List<Job> runningJob = getRunningAndFinishJob();

        Map<String, Job> runningMap = runningJob.stream().collect(Collectors.toMap(Job::getJobId, Function.identity()));
        List<Job.JobTask> runTask = getRunTask();
        log("运行中的:" + runTask.size());
        runTask.forEach(job -> {
            log(job.getName());
        });
        String jobid = "0";
        String jobName = "";
        if (runTask.size() < 15) {
            for (Job job : allJob) {
                if (runningMap.containsKey(job.getId())) {
                    continue;
                }
                jobid = job.getId();
                jobName = job.getJobName();
                break;
            }
            String exJobid = getExJobid(jobid);
            runJobHttp(exJobid);
            //启动作业
            StringBuffer stringBuffer = new StringBuffer("开始自动全量运行作业：\n");
            stringBuffer.append("作业名:");
            stringBuffer.append(jobName);
            stringBuffer.append("\n");
            stringBuffer.append("======");
            stringBuffer.append("正在运行的表格数量:");
            stringBuffer.append(runTask.size());
            stringBuffer.append("\n");
            HttpClientUtil.send(stringBuffer.toString());
        }

    }


    private static void runJobHttp(String exJobid) {
        try {
            String url = "http://home.chail.top:61043/api/v2/job/start/TOTAL/%s";
            url = String.format(url, exJobid);
            String result = HttpClientUtil.doPost(url, null, null, "");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static List<Job> getRunningAndFinishJob() throws SQLException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        String jobSql = "select id,job_name ,job_id,status   from mc_job_history_new  where status in ('RUNNING','RUN_FAILURE','COMPLETED')  order by job_name ";
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, jobSql);
        List<Job> list = new ArrayList<>();
        while (rs.next()) {
            Job job = new Job();
            Db2ObjectUtils.getObj(rs, job, Job.class);
            list.add(job);
        }
        CloseUtils.close(connection, rs);
        return list;
    }


    public static List<Job> getAllJob() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql = "select * from mc_job mj ";
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, jobSql);
        List<Job> list = new ArrayList<>();
        while (rs.next()) {
            Job job = new Job();
            Db2ObjectUtils.getObj(rs, job, Job.class);
            list.add(job);
        }
        CloseUtils.close(connection, rs);
        return list;
    }


    public static List<Job.JobTask> getRunTask() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql = "select * from mc_job_task mjt where task_status ='SYNCHRONIZING'";
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, jobSql);
        List<Job.JobTask> list = new ArrayList<>();
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            list.add(jobTask);
        }
        CloseUtils.close(rs, rs.getStatement(), connection);
        return list;
    }


    public static String getExJobid(String jobId) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql = "select value ,job_id  from mc_job_config mjc  where job_id =%s and name='JOB_EXTERNAL_ID';";
        jobSql = String.format(jobSql, jobId);
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, jobSql);
        List<Job.JobTask> list = new ArrayList<>();
        String exJobId = "0";
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            exJobId = jobTask.getExJobid();
            break;
        }
        CloseUtils.close(connection, rs);
        return exJobId;
    }


}
