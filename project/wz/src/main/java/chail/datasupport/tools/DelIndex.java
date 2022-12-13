package chail.datasupport.tools;

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
 * @date :2022/6/8 16:52
 * @description :
 * @modyified By:
 */
public class DelIndex extends CheckJob{


    public static void main(String[] args) throws SQLException, ClassNotFoundException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        //getCount("259","541");247/533/244/495//246/517//253/509/
        delIndex("253","509");




    }

    public static void delIndex(String jobid,String jobHisId) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String sql="select\n" +
                "'delete from mc_db_index where id='||mdi.id||';' as sql,\n" +
                "mjt.id,\n" +
                "mjt.name,\n" +
                "mjt.job_id ,\n" +
                "mjt.job_history_id ,\n" +
                "mjt.source_schema_name ,\n" +
                "mjt.source_table_name ,\n" +
                "mss.source_id ,\n" +
                "mdi.*\n" +
                "from\n" +
                "mc_job_task mjt ,\n" +
                "mc_sensitive_table mst ,\n" +
                "mc_sensitive_source mss ,\n" +
                "mc_db_index mdi\n" +
                "where\n" +
                "mjt.job_id = %s\n" +
                "and mjt.job_history_id = %s\n" +
                "and mjt.task_step ='TQ'\n" +
                //"and mjt.task_status = 'SYNCHRONIZING'\n" +
                "and mjt.table_id = mst.id\n" +
                "and mst.sensitive_source_id =mss.id\n" +
                "and mdi.source_id =mss.source_id\n" +
                "and mdi.table_name =mjt .source_table_name\n";
        DruidPooledConnection connection = dataSource.getConnection();
        sql=String.format(sql,jobid,jobHisId);
        ResultSet rs = getResult(connection,sql);
        List<Job.JobTask> list=new ArrayList<>();
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
