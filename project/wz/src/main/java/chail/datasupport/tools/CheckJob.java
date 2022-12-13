package chail.datasupport.tools;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import chail.HttpClientUtil;
import com.chail.datasupport.tools.model.Job;
import chail.Source;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author : yangc
 * @date :2022/5/31 16:44
 * @description : 巡检系统
 * @modyified By:
 */
public class CheckJob {


    public static final Logger LOGGER = LogManager.getLogger(CheckJob.class);
    public static final String LOGFileName = "report";

    public static DruidDataSource dataSource;



    public static final String FLAG = "******************************************************";

    static List<Job> allJob=new ArrayList<>();


    public static void getConnection() throws ClassNotFoundException, SQLException {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://pi.chail.top:61009/dm");
        dataSource.setUsername("dm");
        dataSource.setPassword("hzmcdm");
        dataSource.setInitialSize(2);
        dataSource.setMaxActive(5);
        dataSource.setMinIdle(1);
        dataSource.setMaxWait(60000);
        dataSource.setValidationQuery("select version()");
        dataSource.setTestOnBorrow(true);
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnReturn(false);
        dataSource.setPoolPreparedStatements(true);
        dataSource.setTimeBetweenEvictionRunsMillis(6000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        getConnection();
        //createTable();
        //getCount();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    exec();
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (IntrospectionException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }, 5000, 600000);



        //getCount();

    }


    public static void exec() throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        //获取连接
        List<Job> runningJob = getRunningJob();
        logTitle("总作业");
        allJob = getAllJob();

//        logTitle("所有正在运行的作业");
//        for (Job job : runningJob) {
//            log(String.format("jobHisId:%s ,jobId:%s ,jobName:%s ,jobStatus:%s", job.getId(), job.getJobId(), job.getJobName(), job.getStatus()));
//        }


        log(FLAG);
        logTitle("全量==完成");
        getTQFinish(runningJob);


        log(FLAG);
        logTitle("运行异常的作业");
        List<Job> failureJobs = new ArrayList<>();
        for (Job job : runningJob) {
            if ("RUN_FAILURE".equals(job.getStatus())) {
                log(String.format("jobHisId:%s ,jobId:%s ,jobName:%s ,jobStatus:%s", job.getId(), job.getJobId(), job.getJobName(), job.getStatus()));
                failureJobs.add(job);
            }
        }

        logTitle("==没有运行的表格==");
        getTQNotRunning(failureJobs);


        logTitle("==异常的表格==");
        getFail(failureJobs);


        logTitle("==未开始的表格==");
        getTQDefault(runningJob);


        log(FLAG);
        logTitle("全量==卡住超过1h");
        tq1HFail(runningJob);

        //
        log(FLAG);
        logTitle("全量==用Agent");
        engineAgent(runningJob);


        //发微信
        List<String> logs = WriteMessegeToLog.logs;
        StringBuffer stringBuffer = new StringBuffer();
        int num=0;
        for (int i = 0; i < logs.size(); i++) {
            String s = logs.get(i);
            num+=s.length();
            if(num>2000){


                HttpClientUtil.send(stringBuffer.toString());
                stringBuffer.setLength(0);
                num=0;
            }
            stringBuffer.append(s + "\n");
        }
        WriteMessegeToLog.logs.clear();
    }

    public static List<Job.JobTask> getTQ(List<Job> runningJob) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job.JobTask> list = new ArrayList<>();
        DruidPooledConnection connection = dataSource.getConnection();
        for (Job job : runningJob) {
            String jobSql = String.format("select * from mc_job_task where job_id = %s and job_history_id = %s and task_step  = 'TQ'  ", job.getJobId(), job.getId());
            ResultSet rs = getResult(connection,jobSql);
            while (rs.next()) {
                Job.JobTask jobTask = new Job.JobTask();
                jobTask.setJobId(job.getId());
                jobTask.setJobHisId(job.getJobId());
                Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
                list.add(jobTask);
            }
            CloseUtils.close(connection,rs);
        }
        printGroupBy(list,false);
        return list;
    }

    public static List<Job.JobTask> getTQDefault(List<Job> runningJob) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job.JobTask> list = new ArrayList<>();
        DruidPooledConnection connection = dataSource.getConnection();
        for (Job job : runningJob) {
            String jobSql = String.format("select * from mc_job_task where job_id = %s and job_history_id = %s and task_status = 'DEFAULT' ", job.getJobId(), job.getId());
            ResultSet rs = getResult(connection,jobSql);
            while (rs.next()) {
                Job.JobTask jobTask = new Job.JobTask();
                jobTask.setJobId(job.getId());
                jobTask.setJobHisId(job.getJobId());
                Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
                list.add(jobTask);
            }
            CloseUtils.close(connection,rs);
        }
        printGroupBy(list,false);
        return list;
    }


    public static List<Job.JobTask> getTQNotRunning(List<Job> runningJob) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job.JobTask> list = new ArrayList<>();
        DruidPooledConnection connection = dataSource.getConnection();
        for (Job job : runningJob) {
            String jobSql = String.format("select * from mc_job_task where job_id = %s and job_history_id = %s and task_status not in ( 'SYNCHRONIZING' ,'DEFAULT' )", job.getJobId(), job.getId());
            ResultSet rs = getResult(connection,jobSql);
            while (rs.next()) {
                Job.JobTask jobTask = new Job.JobTask();
                jobTask.setJobId(job.getId());
                jobTask.setJobHisId(job.getJobId());
                Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
                list.add(jobTask);
            }
            CloseUtils.close(connection,rs);
        }
        printGroupBy(list,false);

        return list;
    }




    public static  List<Job>  getAllJob() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql = 	"select * from mc_job mj where config  ='CONTINUOUS_INCREMENT,TOTAL_QUANTITY,INC_RT_QUANTITY'";
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


    public static List<Job.JobTask> tq1HFail(List<Job> runningJob) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job.JobTask> list = new ArrayList<>();
        DruidPooledConnection connection = dataSource.getConnection();
        for (Job job : runningJob) {
            String jobSql = String.format("select  date_part('day',now()-last_modify_time)as diff_d,date_part('hour',now()-last_modify_time) as diff_h,last_modify_time ,* from mc_job_task  where job_id =%s and job_history_id =%s and task_step ='TQ' and task_status  ='SYNCHRONIZING' order by diff_d desc,diff_h desc", job.getJobId(), job.getId());
            ResultSet rs = getResult(connection,jobSql);
            while (rs.next()) {
                Job.JobTask jobTask = new Job.JobTask();
                jobTask.setJobId(job.getId());
                jobTask.setJobHisId(job.getJobId());
                Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
                if(jobTask.getDiffDay().equals("0.0")&&jobTask.getDiffHour().equals("0.0")){
                    continue;
                }
                list.add(jobTask);
            }
            CloseUtils.close(connection,rs);
        }
        printGroupBy(list,false);
        return list;
    }







    public static void getFail(List<Job> failureJobs) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job.JobTask> list = new ArrayList<>();
        DruidPooledConnection connection = dataSource.getConnection();
        for (Job job : failureJobs) {
            String jobSql = String.format("select * from mc_job_task mjt where job_id =%s and job_history_id =%s  and task_status not in ('SYNCHRONIZING')", job.getJobId(), job.getId());
            ResultSet rs = getResult(connection,jobSql);
            while (rs.next()) {
                Job.JobTask jobTask = new Job.JobTask();
                Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
                jobTask.setJobId(job.getId());
                jobTask.setJobHisId(job.getJobId());
                list.add(jobTask);
            }
            CloseUtils.close(connection,rs);
        }
        printGroupBy(list,false);
    }


    public static void printGroupBy(List<Job.JobTask> list, boolean count){
        Map<String, List<Job.JobTask>> collect = list.stream().collect(Collectors.groupingBy(Job.JobTask::getJobId));

        logTitle("作业数:"+collect.size());
        collect.forEach((s, jobTasks) -> {
            Job.JobTask jobTask1 = jobTasks.get(0);
            logTitle(String.format("===NAME: %s JID:%s  JHID: %s ",jobTask1.getName(),jobTask1.getJobId(),jobTask1.getJobHisId()));
            for (Job.JobTask jobTask : jobTasks) {
                if(count){
                    try {
                        jobTask.setCount(getSourceCount(jobTask));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                String countS = jobTask.getCount() != null ? "count: " + jobTask.getCount()+"---success:"+jobTask.getTqSucess() : "";
                log(String.format(" %s --> %s ",  jobTask.getName().substring(0,jobTask.getName().lastIndexOf("@")), jobTask.getTSchemaAndTableName())+countS);
            }
        });



    }

    public static List<Job.JobTask> engineAgent(List<Job> runningJob) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job.JobTask> list = new ArrayList<>();
        DruidPooledConnection connection = dataSource.getConnection();
        for (Job job : runningJob) {
            String sql="select * from mc_job_task where id in (select cast ( executor_param as integer )    from xxl_job_info xji  where job_group =2) and task_step  ='TQ' and job_id =%s and job_history_id =%s order by name";
            sql=String.format(sql,job.getJobId(),job.getId());
            ResultSet rs = getResult(connection,sql);
            while (rs.next()) {
                Job.JobTask jobTask = new Job.JobTask();
                Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
                list.add(jobTask);
            }
            CloseUtils.close(connection,rs);
        }
        printGroupBy(list,false);
        return list;


    }


    public static  List<Job>  getTQFinish(List<Job> runningJob) throws SQLException {
        int i = 0;
        List<Job> jobs=new ArrayList<>();
        DruidPooledConnection connection = dataSource.getConnection();
        for (Job job : runningJob) {
            String jobSql = String.format("select * from mc_job_task mjt where job_id =%s and job_history_id =%s and task_step ='TQ'", job.getJobId(), job.getId());
            ResultSet rs = getResult(connection,jobSql);
            if (!rs.next()) {
                log(job.getJobName());
                jobs.add(job);
                i++;
            }
            CloseUtils.close(connection,rs);
        }
        log("==作业数:"+runningJob.size()+"==全量完成数 " + i + " 个======");
        return jobs;
    }


    public static List<Job> getRunningJob( ) throws SQLException, IllegalAccessException, IntrospectionException, InvocationTargetException {
        String jobSql = "select id,job_name ,job_id,status   from mc_job_history_new  where status in ('RUNNING','RUN_FAILURE')  order by job_name ";
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


    public static String getSourceCount(Job.JobTask jobTask ) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
            String jobId = jobTask.getJobId();
            String jobSql=String.format("select * from mc_source ms where id =( select sor.source_id from mc_job job left join mc_sensitive_source sor on sor.id = job.input_id where job.id = %s )",jobId);
            String url="http://home.chail.top:61010/setting/utils/sql/exec";
            DruidPooledConnection connection = dataSource.getConnection();
            ResultSet rs = getResult(connection,jobSql);
            while (rs.next()) {
                Source sour = new Source();
                Db2ObjectUtils.getObj(rs, sour, Source.class);
                Map<String,String> map=new HashMap<>();
                map.put("sourcename",sour.getSourceName());
                map.put("sourceId",sour.getId());
                map.put("sql","select count(1) from "+jobTask.getSSchemaAndTableName());
                String result = HttpClientUtil.doPost(url, null, null, JSONObject.toJSONString(map));
                JSONObject jsonObject = JSONObject.parseObject(result);
                JSONArray data = (JSONArray) jsonObject.get("data");
                String count =data.get(1).toString();
                CloseUtils.close(connection,rs);
                return count;
            }
            return "";

    }


    public static void getCount() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql = String.format("select * from mc_job_task mjt where job_id =%s and job_history_id =%s and task_step ='TQ'", "256", "552");
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection,jobSql);
        List<Job.JobTask> list=new ArrayList<>();
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            String sourceCount = getSourceCount(jobTask);
            sourceCount=sourceCount.substring(2,sourceCount.length()-2);
            jobTask.setCount(sourceCount);
            list.add(jobTask);
        }
        CloseUtils.close(connection,rs);
        list.sort(Comparator.comparingInt(o -> Integer.valueOf(o.getCount())));
        System.out.println("===================================");
        list.forEach(ob-> System.out.println(ob.getSSchemaAndTableName()+"-----"+ob.getCount()+"======"+ob.getTqSucess()));
    }


    public static ResultSet getResult(Connection connection,String sql) throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return rs;
    }


    public static void logTitle(String meg) {
        log("=====" + meg + "=====");
    }

    public static void log(String meg) {
        WriteMessegeToLog.writeToLog(meg, LOGFileName);
        //LOGGER.info(meg);
    }


    public static void createTable() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql =" select * from mc_db_object_sql mdos right join mc_job_task mjt on mdos.table_name " +
                "=mjt.source_table_name and mdos.schema_name =mjt.source_schema_name where source_id in(" +
                " select mss.source_id from mc_job job, mc_sensitive_source mss where job.input_id = mss.id and job.name like '乐清人民%' )" +
                " and mdos.object_type='TABLE' and mjt.job_id =256 and mjt.job_history_id =552";
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection,jobSql);
        List<Job.JobTask> list=new ArrayList<>();
        String add=", \n"+
                "\"ods_insert_time\" DATE DEFAULT sysdate, \n" +
                "\"ods_update_time\" TIMESTAMP (6), \n" +
                "\"ods_delete_flag\" CHAR(1), \n" +
                "\"source_change_time\" TIMESTAMP (6), \n" +
                "\"ods_oper_type\" CHAR(1));";
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            String sql=jobTask.getSql().replace("${TABLE_NAME}",jobTask.getTargetSchema()+"."+jobTask.getTargetTableName());
            sql = sql.replace("in ${MC_TABLE_SPACE_NAME}", "");
            sql= sql.replace(");",add);

            //String drop=String.format("drop table %s.%s;",jobTask.getTargetSchema(),jobTask.getTargetTableName());
            sql="\n"+sql;
            jobTask.setSql(sql);
            list.add(jobTask);
        }
        for (Job.JobTask jobTask : list) {
            System.out.println(jobTask.getSql());
        }



    }







}
