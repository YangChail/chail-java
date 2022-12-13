package chail.datasupport.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.chail.datasupport.tools.model.Job;
import com.chail.sftp.SftpUtil;

import java.beans.IntrospectionException;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : yangc
 * @date :2022/6/29 13:52
 * @description :
 * @modyified By:
 */
public class DownLoadLogs extends Ds2 {

    public static void main(String[] args) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        getConnection();
        List<Job.JobTask> errorTasks = getErrorTasks();
        downLoadLog(errorTasks);

    }


    public static void downLoadLog(List<Job.JobTask> errorTasks) {
        String path = "/dm/app/datasupport-common/logs/engine/{jobName}/history{hisId}/{targetSchema.targetTable}-jobtask{taskId}/";
        deleteDirectory("D:"+File.separator+"wz_log"+File.separator);
        for (Job.JobTask errorTask : errorTasks) {
            String remotePath = path.replace("{jobName}", errorTask.getJobName()).replace("{hisId}", errorTask.getJobHisId())
                    .replace("{targetSchema.targetTable}", errorTask.getTSchemaAndTableName()).replace("{taskId}", errorTask.getTaskId());

            String localFile = "D:%swz_log%s{jobName}%shistory{hisId}%s{targetSchema.targetTable}-jobtask{taskId}";
            localFile = String.format(localFile, File.separator, File.separator, File.separator, File.separator);

            localFile = localFile.replace("{jobName}", errorTask.getJobName()).replace("{hisId}", errorTask.getJobHisId())
                    .replace("{targetSchema.targetTable}", errorTask.getTSchemaAndTableName()).replace("{taskId}", errorTask.getTaskId());

            try{
                sftp(remotePath,localFile,false);
            }catch (Exception e){
                try{
                    sftp(remotePath,localFile,true);
                }catch (Exception ee){

                }
            }

        }
    }


    private static void sftp(String remotePath, String localFile,boolean is195){
        SftpUtil sftpUtil = new SftpUtil();
        if(is195){
            sftpUtil = new SftpUtil("home.chail.top",61047);
        }
        List<String> strings = sftpUtil.listFiles(remotePath);
        for (String fileName : strings) {
            if (fileName.equals(".") || fileName.equals("src/main")) {
                continue;
            }
            File file = new File(localFile);
            if (!file.exists()) {
                file.mkdirs();
            }
            String l = localFile + File.separator + fileName;
            String t = remotePath + "/" + fileName;
            sftpUtil.downloadFile(l, t);
        }

    }

    public static List<Job.JobTask> getErrorTasks() throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        List<Job.JobTask> list = new ArrayList<>();
        String sql = "select\n" +
                "\tmjt.target_schema_name as target_schema_name,\n" +
                "\tstb.\"name\" as target_table_name,\n" +
                "\tmjt.task_status as status,\n" +
                "\tmjt.job_id as job_id,\n" +
                "\tmjt.job_history_id as job_history_id,\n" +
                "\tjob.display_name as job_name,\n" +
                "\tmjt.id as task_id\n" +
                "from\n" +
                "\t(\n" +
                "\tselect\n" +
                "\t\tmax(last_modify_time) as last_modify_time,\n" +
                "\t\ttable_id as table_id\n" +
                "\tfrom\n" +
                "\t\tmc_job_task\n" +
                "\tgroup by\n" +
                "\t\ttable_id) as a\n" +
                "left join mc_job_task mjt on\n" +
                "\tmjt.table_id = a.table_id\n" +
                "\tand mjt.last_modify_time = a.last_modify_time\n" +
                "right join mc_sensitive_table as stb on\n" +
                "\tmjt.table_id = stb.id\n" +
                "left join mc_job job on\n" +
                "\tmjt.job_id = job.id\n" +
                "left join mc_sensitive_source as senstive_source on\n" +
                "\tsenstive_source.id = stb.sensitive_source_id\n" +
                "where\n" +
                "\tstb.rel_id is not null\n" +
                "\tand mjt.task_status = 'FAILURE'\n" +
                "    \n" +
                "      order by\n" +
                "        mjt.task_status,\n" +
                "        mjt.last_modify_time";
        String jobSql = String.format(sql);
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection, jobSql);
        while (rs.next()) {
            Job.JobTask jobTask = new Job.JobTask();
            Db2ObjectUtils.getObj(rs, jobTask, Job.JobTask.class);
            list.add(jobTask);
        }
        CloseUtils.close(rs);
        return list;
    }




    /**
     * 删除文件夹
     * 删除文件夹需要把包含的文件及文件夹先删除，才能成功
     *
     * @param directory 文件夹名
     * @return 删除成功返回true,失败返回false
     */
    public static boolean deleteDirectory(String directory) {
        // directory不以文件分隔符（/或\）结尾时，自动添加文件分隔符，不同系统下File.separator方法会自动添加相应的分隔符
        if (!directory.endsWith(File.separator)) {
            directory = directory + File.separator;
        }
        File directoryFile = new File(directory);
        // 判断directory对应的文件是否存在，或者是否是一个文件夹
        if (!directoryFile.exists() || !directoryFile.isDirectory()) {
            System.out.println("文件夹删除失败，文件夹不存在" + directory);
            return false;
        }
        boolean flag = true;
        // 删除文件夹下的所有文件和文件夹
        File[] files = directoryFile.listFiles();
        for (int i = 0; i < files.length; i++) {  // 循环删除所有的子文件及子文件夹
            // 删除子文件
            if (files[i].isFile()) {
                flag = deleteFile(files[i].getAbsolutePath());
                if (!flag) {
                    break;
                }
            } else {  // 删除子文件夹
                flag = deleteDirectory(files[i].getAbsolutePath());
                if (!flag) {
                    break;
                }
            }
        }

        if (!flag) {
            System.out.println("删除失败");
            return false;
        }
        // 最后删除当前文件夹
        if (directoryFile.delete()) {
            System.out.println("删除成功：" + directory);
            return true;
        } else {
            System.out.println("删除失败：" + directory);
            return false;
        }
    }
    /**
     * 删除文件
     *
     * @param fileName 文件名
     * @return 删除成功返回true,失败返回false
     */
    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        if (file.isFile() && file.exists()) {
            file.delete();
            System.out.println("删除文件成功：" + fileName);
            return true;
        } else {
            System.out.println("删除文件失败：" + fileName);
            return false;
        }
    }
}
