package org.chail;

import lombok.extern.slf4j.Slf4j;
import org.chail.jedis.NameSpaceCmd;
import org.chail.model.JobTask;
import org.chail.util.JDBCUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class TransferRedis {

    public  static  final String LOCAL_IP=System.getProperty("local","false").equals("true")?"192.168.51.194":"127.0.0.1";

    public  static String HIVE_IP=System.getProperty("local","false").equals("true")?"192.168.51.241":"10.15.15.3";

    public  static String HIVE_USER=System.getProperty("local","false").equals("true")?"meichuang":"meichuang";

    public  static String HIVE_PASS=System.getProperty("local","false").equals("true")?"hzmc123456":"meichuang123456";

    private static final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);


    private static final String[] tables = {
            "13_HISDB.dbo_Admission",
            "13_HISDB.dbo_AdmissionExtInfo",
            "13_HISDB.dbo_AdmitPre",
            "13_HISDB.dbo_AllergyReg",
            "13_HISDB.dbo_BASY_AllergyRecord",
            "13_HISDB.dbo_BASY_BloodRecord",
            "13_HISDB.dbo_BASY_DiagMatch",
            "13_HISDB.dbo_BASY_DischargeDiag",
            "13_HISDB.dbo_BASY_EmployeeSign",
            "13_HISDB.dbo_BASY_PatInfoMain",
            "13_HISDB.dbo_BASY_ReportDischargeDiag",
            "13_HISDB.dbo_BASY_ReportMain",
            "13_HISDB.dbo_BASY_ReportSurgeryRecord",
            "13_HISDB.dbo_BASY_SSDMK",
            "13_HISDB.dbo_BASY_SurgeryRecord",
            "13_HISDB.dbo_BASY_ZDDMK",
            "13_HISDB.dbo_BASY_ZYMainRecord",
            "13_HISDB.dbo_BasicData",
            "13_HISDB.dbo_BloodApply",
            "13_HISDB.dbo_BloodApplyDetail",
            "13_HISDB.dbo_BloodMatingTest",
            "13_HISDB.dbo_BloodReceivePut",
            "13_HISDB.dbo_BloodTest",
            "13_HISDB.dbo_BloodTransfusionBook",
            "13_HISDB.dbo_BloodTransfusionDetail",
            "13_HISDB.dbo_BloodWasteBook",
            "13_HISDB.dbo_Charge",
            "13_HISDB.dbo_DailyCheckTemp",
            "13_HISDB.dbo_DrugApplyMZ",
            "13_HISDB.dbo_DrugBasic",
            "13_HISDB.dbo_DrugCheckCode",
            "13_HISDB.dbo_DrugDeptApply",
            "13_HISDB.dbo_DrugInOutReport",
            "13_HISDB.dbo_DrugOutMZ",
            "13_HISDB.dbo_DrugOutZY",
            "13_HISDB.dbo_DrugOut_SJK"
    };

    public static void main(String[] args) throws Exception {
        TransferRedis transferRedis=new TransferRedis();
        transferRedis.redisToRedis();

    }


    private void redisToRedis(){
        List<String> TableList = Arrays.asList(tables);
        log.info("开始");
        Jedis  jedisNew = new Jedis(LOCAL_IP, NameSpaceCmd.DEFAULT_PORT );
        jedisNew.auth("mcadmin");
        Jedis  jedisOld = new Jedis(LOCAL_IP, NameSpaceCmd.DEFAULT_PORT + 10000);
        jedisOld.auth("mcadmin");
        //拿到所有
        NameSpaceCmd nameSpaceCmd=new NameSpaceCmd(jedisNew);
        List<String> all = nameSpaceCmd.getAll();
        List<String> collect = all.stream().distinct().collect(Collectors.toList());
        log.info("-----开始转移：{}-----",collect.size());
        int i=0;
        for (String nameSpace : collect) {
            if("__namespace".equals(nameSpace)||"mcadmin".equals(nameSpace)){
                i++;
                continue;
            }
            if(TableList.contains(nameSpace)){
                log.info("-----开始转移：{}已经转移过了-----",nameSpace);
                i++;
                jedisNew.auth(nameSpace);
                jedisNew.set("transfer_finish","true");
                continue;
            }
            log.info("-----开始转移：{}-剩余{}----",nameSpace,collect.size()-i);
            try{
                jedisNew.auth(nameSpace);
                jedisOld.auth(nameSpace);
                ScanParams params = new ScanParams().count(1000);
                String cursorKey="nameSpace_transfer_redis";
                String cursor = "0";
                String cursorKeyValue = jedisOld.get(cursorKey);
                if(cursorKeyValue!=null&& !cursorKeyValue.isEmpty()){
                    cursor = cursorKeyValue;
                }
                long num=0;
                do {
                    ScanResult<String> result = jedisOld.scan(cursor, params);
                    List<String> keys = result.getResult();
                    for (String key : keys) {
                        String value = jedisOld.get(key);
                        if(value.indexOf("-")>0){
                            log.info("-----开始转移：{},数据有问题change-----",nameSpace);
                            long time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(value).getTime();
                            jedisNew.set(key,time+"");
                        }
                        jedisNew.setnx(key,value);
                        num++;
                    }
                    if(num%10000==0){
                        log.info("-----开始转移：{}--{}条---",nameSpace,num);
                    }
                    cursor = result.getCursor();
                    if(!cursor.equals("0")){
                        jedisOld.set(cursorKey,cursor);
                    }
                } while (!cursor.equals("0"));
                log.info("-----完成转移：{}--{}条---",nameSpace,num);
                i++;
                jedisNew.set("transfer_finish","true");
            }catch (Throwable e){
                log.error("-----开始转移：{}--error---",nameSpace,e);
            }


        }

    }



    private void dbToRedis() throws InterruptedException {
        while (true) {
            TransferRedis transferRedis = new TransferRedis();
            List<JobTask> jobTaskAll = transferRedis.getData();
            log.info("获取到 task:{}", jobTaskAll.size());
            CountDownLatch taskLatch = new CountDownLatch(jobTaskAll.size());
            for (JobTask task : jobTaskAll) {
                CompletableFuture.supplyAsync(() -> {
                    transferRedis.transfer(task);
                    taskLatch.countDown();
                    return null;
                }, threadPoolExecutor).exceptionally(e -> {
                    log.error("{}:hive transfer error", task.getId(), e);
                    taskLatch.countDown();
                    return null;
                });
            }
            taskLatch.await();
            transferRedis.setLastDay(transferRedis.getLastDay());
            Thread.sleep(1000);
        }
    }






    private  List<JobTask> getData()  {
        JDBCUtil pgJdbc = getPgJdbc();
        pgJdbc.getConnection();
        List<JobTask> jobTasks = new ArrayList<>();
        try {
            String sql = "select id from mc_job_history_new where (status = 'RUNNING' or status='RUN_FAILURE' ) and job_id in( select mj.id from mc_job mj left join mc_source s on s.id = mj.output_id where s.sub_source_type = 'HIVE')";
            List<Map<String, Object>> maps = null;
            maps = pgJdbc.executeQuery(sql);
            List<Integer> ids = new ArrayList<Integer>();
            for (Map<String, Object> map : maps) {
                Long id = (Long) map.get("id");
                ids.add(Math.toIntExact(id));
            }
            for (Integer id : ids) {
                sql = String.format("select * from mc_job_task mjt where task_step ='INC' and task_status ='SYNCHRONIZING' and job_history_id =%s", id);
                maps = pgJdbc.executeQuery(sql);
                for (Map<String, Object> map : maps) {
                    String sourceSchemaName = (String) map.get("source_schema_name");
                    String sourceTableName = (String) map.get("source_table_name");
                    String targetSchemaName = (String) map.get("target_schema_name");
                    String targetTableName = (String) map.get("target_table_name");
                    Long taskId = (Long) map.get("id");
                    Long hisId= (Long) map.get("job_history_id");

                    JobTask build = JobTask.builder().sourceSchemaName(sourceSchemaName)
                            .sourceTableName(sourceTableName)
                            .id(taskId)
                            .targetSchemaName(targetSchemaName)
                            .targetTableName(targetTableName)
                            .jobHistoryId(hisId)
                            .build();
                   jobTasks.add(build);
               }
            }
        } catch (Exception e) {
           log.error("{}",e);
        }finally {
            pgJdbc.releaseConnectn();
        }
        return jobTasks;
    }


    private String getLastDay() {
        Jedis jedis = null;
        try {
            jedis = new Jedis(LOCAL_IP, NameSpaceCmd.DEFAULT_PORT + 10000);
            jedis.auth("mcadmin");
            String s = jedis.get("hiveTransferLastDay");
            if (isNotEmpty(s)) {
                return s;
            }
            return "20240101";
        } catch (Exception e) {
            log.error("{}", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return "20240101";
    }


    private void setLastDay(String lastDay) {
        Jedis jedis = null;
        try {
            Date date = new SimpleDateFormat("yyyyMMdd").parse(lastDay);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.DAY_OF_MONTH, -1);
            Date newDate = calendar.getTime();
            String newDateString = new SimpleDateFormat("yyyyMMdd").format(newDate);
            jedis = new Jedis(LOCAL_IP, NameSpaceCmd.DEFAULT_PORT + 10000);
            jedis.auth("mcadmin");
           jedis.set("hiveTransferLastDay",newDateString);
        } catch (Exception e) {
            log.error("{}", e);
        } finally {
            if(jedis!=null){
                jedis.close();
            }
        }
    }

    private boolean isNotEmpty(String str){
        if(str!=null&&str.length()>0){
            return true;
        }else{
            return false;
        }
    }

    private void transfer(JobTask jobTask){
        Jedis jedis = new Jedis(LOCAL_IP, NameSpaceCmd.DEFAULT_PORT+10000);
        jedis.auth("mcadmin");
        String nameSpace =jobTask.getJobHistoryId() + "_" + jobTask.getSourceSchemaName() + "_" + jobTask.getSourceTableName();
        NameSpaceCmd nameSpaceCmd=new NameSpaceCmd(jedis);
        try{
            nameSpaceCmd.add(nameSpace);
        }catch (Exception e){
            log.warn("hive-transfer-data-存在nameSpace,忽略");
        }
        jedis.auth(nameSpace);
        String lastDay = getLastDay();
        String key = "hive_transfer_" + jobTask.getId();
        String mergeDate = jedis.get(key);
        if (isNotEmpty(mergeDate)) {
            if (mergeDate.contains(lastDay)) {
                log.info("hive-transfer-data-{}-{}分区已经转移过，跳过...", jobTask.getId(), lastDay);
                return;
            }
        }
        String targetSchemaName = jobTask.getTargetSchemaName();
        String targetTableName = jobTask.getTargetTableName();
        JDBCUtil jdbc = getJdbc();
        jdbc.getConnection();
        try {
            List<String> partitions = getPartitions(jdbc, targetSchemaName, targetTableName);
            //查看所有分区
            if (!partitions.contains(lastDay)) {
                log.info("hive-transfer-data-转移数据-{},不包含分区{}跳过...", targetSchemaName + "." + targetTableName, lastDay);
                return;
            }
            List<String> values = getValues(jdbc, targetSchemaName, targetTableName, lastDay);
            values.forEach(value -> {
                String[] split = value.split(",");
                String s = split[0];
                String v = split[1];
                jedis.set(s,v);
            });
            log.info("hive-transfer-data-转移数据-{},-{}转移成功{}条...", targetSchemaName + "." + targetTableName, lastDay,values.size());
        } catch (Exception e) {
            log.error("获取分区异常", e);
        }finally {
            jedis.close();
            jdbc.releaseConnectn();
        }

    }


    private  List<String> getPartitions(JDBCUtil jdbc,String targetSchemaName, String targetTableName) throws Exception {
        String showPartitions = String.format("show partitions %s.%s", targetSchemaName, targetTableName);
        List<Map<String, Object>> maps = jdbc.executeQuery(showPartitions );
        List<String> partitions = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            String partition = (String) map.get("partition");
            partition = partition.replace("mc_dt" + "=", "");
            partitions.add(partition);
        }
        return partitions;
    }


    private List<String> getValues(JDBCUtil jdbc,String targetSchemaName, String targetTableName, String lastDay) throws Exception {
        List<String> values = new ArrayList<>();
        String sql = String.format("select distinct hash_key_column,ods_insert_time from %s.%s where mc_dt='%s'", targetSchemaName, targetTableName, lastDay);
        List<Map<String, Object>> maps = jdbc.executeQuery(sql );
        log.info("{}-execute sql : {}", "", sql);
        for (Map<String, Object> map : maps) {
            Object hashKeyColumn = map.get("hash_key_column");
            Date ods_insert_time = (Date) map.get("ods_insert_time");
            values.add(hashKeyColumn + "," + ods_insert_time.getTime());
        }
        return values;
    }



    public  JDBCUtil getJdbc() {
        String user = HIVE_USER;
        String pass = HIVE_PASS;
        //String url = "jdbc:hive2://192.168.241.104:10000/default";
        String url = "jdbc:hive2://"+HIVE_IP+":10000/default";
        return new JDBCUtil(url, "org.apache.hive.jdbc.HiveDriver", user, pass);
    }



    public  JDBCUtil getPgJdbc() {
        String user = "dataflow";
        String pass = "Hzmc321#";
        //String url = "jdbc:hive2://192.168.241.104:10000/default";
        String url = "jdbc:postgresql://"+LOCAL_IP+":45432/dataflow";
        return new JDBCUtil(url, "org.postgresql.Driver", user, pass);
    }

}