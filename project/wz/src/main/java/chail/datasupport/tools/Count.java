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

/**
 * @author : yangc
 * @date :2022/6/8 15:39
 * @description :
 * @modyified By:
 */
public class Count extends CheckJob {

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        //getCount("259","541");/247/533//263/568/244/495/246/517/263/568/244/495//254/534246/517//245/573//233/488/254/534//233/488//247/577//247/577/237/613/
        getConnection();
        getCount("247", "577");

//        REPAIRING
//        DEFAULT
//        SUCCESS
//        FAILURE
//        MANUAL_STOP
//        SYNCHRONIZING
//===================================
//HIS4.YZ_YIZHUZX-----11743391======0

    }

    public static void getCount(String jobid, String jobHisId) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String jobSql = String.format("select * from mc_job_task mjt where job_id =%s and job_history_id =%s and task_step ='TQ' and task_status ='SYNCHRONIZING'", jobid, jobHisId);
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, jobSql);
        List<Job.JobTask> list = new ArrayList<>();
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            String sourceCount = getSourceCount(jobTask);
            sourceCount = sourceCount.substring(2, sourceCount.length() - 2);
            jobTask.setCount(sourceCount);
            System.out.println(sourceCount);
            list.add(jobTask);
        }

        list.sort(Comparator.comparingInt(o -> Integer.valueOf(o.getCount())));
        System.out.println("===================================");
        list.forEach(ob -> System.out.println(ob.getSSchemaAndTableName() + "-----" + ob.getCount() + "======" + ob.getTqSucess()));
    }
}
