package chail.datasupport.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.chail.datasupport.tools.model.Job;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author : yangc
 * @date :2022/6/13 13:47
 * @description :
 * @modyified By:
 */
public class IncCheck extends CheckJob {


    public static void main(String[] args) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
//        List<Job> runningJob = getRunningJob(connection);
//        incFail(runningJob);
    }


    public static List<Job.JobTask> incFail(List<Job> runningJob) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job.JobTask> listAll = new ArrayList<>();
        for (Job job : runningJob) {
            List<Job.JobTask> list = new ArrayList<>();
            String jobSql = String.format("select  date_part('day',now()-last_modify_time)as diff_d,date_part('hour',now()-last_modify_time) as diff_h,last_modify_time ,* from mc_job_task  where job_id =%s and job_history_id =%s and task_step ='INC' and task_status  ='SYNCHRONIZING' order by last_modify_time  desc", job.getJobId(), job.getId());
            DruidPooledConnection connection = dataSource.getConnection();
            ResultSet rs = getResult(connection,jobSql);
            boolean hasInc = false;
            while (rs.next()) {
                Job.JobTask jobTask = new Job.JobTask();
                jobTask.setJobId(job.getId());
                jobTask.setJobHisId(job.getJobId());
                Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
                if (jobTask.getDiffDay().equals("0.0") && jobTask.getDiffHour().equals("0.0")) {
                    hasInc = true;
                    break;
                }
                list.add(jobTask);
            }
            CloseUtils.close(rs);
            if (!hasInc) {
                listAll.add(list.get(0));
                System.out.println();
            }
        }
        printGroupBy(listAll, false);
        return listAll;
    }


    public static void printGroupBy(List<Job.JobTask> list, boolean count) {
        Map<String, List<Job.JobTask>> collect = list.stream().collect(Collectors.groupingBy(Job.JobTask::getJobId));
        logTitle("作业数:" + collect.size());
        collect.forEach((s, jobTasks) -> {
            Job.JobTask jobTask1 = jobTasks.get(0);
            logTitle(String.format("===NAME: %s JID:%s  JHID: %s  Last %s", jobTask1.getName(), jobTask1.getJobId(), jobTask1.getJobHisId(),jobTask1.getLastModifyTime()));

        });
    }
}
