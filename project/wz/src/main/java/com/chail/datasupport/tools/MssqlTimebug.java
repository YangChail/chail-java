package com.chail.datasupport.tools;


import com.chail.DbObject;
import com.chail.datasupport.tools.model.Job;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: yangc
 * @Date: 2022/1/13 9:43
 */
public class MssqlTimebug {

    //public static String url_encrypt = "jdbc:jtds:sqlserver://192.168.225.30:1434/master";

    private  static Map<String,String> schemaMap=new HashMap<>();
    public static void main(String[] args) throws Exception {
        List<DbObject> indexSql = getIndexSql("233","294");

    }


    private static List<DbObject>  getIndexSql(String jobId,String jobHistoryId) throws ClassNotFoundException, SQLException {
        String url = "jdbc:postgresql://10.65.98.77:5432/dm";
        String user = "dm";
        String password = "hzmcdm";
        String driver = "org.postgresql.Driver";
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement();
        String jobSql="select id,job_name ,job_id   from mc_job_history_new  where status in ('RUNNING','RUN_FAILURE')";
        ResultSet rs = stmt.executeQuery(jobSql);
        while (rs.next()){
            String s = rs.getString(1);
            String t = rs.getString(2);
            String j = rs.getString(3);
            Job job=new Job();
            job.setId(s);
            job.setJobId(j);
            job.setJobName(t);
        }


        rs = stmt.executeQuery("select source_schema_name ,target_schema_name  from mc_job_task mjt where job_id ="+jobId+" and job_history_id = "+jobHistoryId);
        while (rs.next()){
            String s = rs.getString(1);
            String t = rs.getString(2);
            schemaMap.put(s,t);
        }
        String sql="select obj.schema_name ,obj .table_name ,obj.\"name\" ,obj.object_type ,obj .\"sql\" from mc_db_object_sql obj where source_id = ( select sour .source_id as input_id from mc_job job , mc_sensitive_source sour where job.id = "+jobId+" and sour.id = job .input_id) and object_type in('INDEX','UNIQUE_INDEX') and table_name in( select source_table_name from mc_job_task mjt where job_id = "+jobId+" and job_history_id = "+jobHistoryId+" ) order by schema_name ,table_name ,object_type";
        rs = stmt.executeQuery(sql);
        List<DbObject> objs=new ArrayList<>();
        while (rs.next()){
            DbObject dbObject=new DbObject();
            dbObject.setSchema(rs.getString(1));
            dbObject.setTableName(rs.getString(2));
            dbObject.setIndexName(rs.getString(3));
            dbObject.setObjectType(rs.getString(4));
            dbObject.setSql(rs.getString(5));
            objs.add(dbObject);
        }
        List<DbObject> objres=new ArrayList<>();
        for (DbObject obj : objs) {
            String objectType = obj.getObjectType();
            String sql1 = obj.getSql();
            if("UNIQUE_INDEX".equals(objectType)){
                sql1 = sql1.replace("UNIQUE INDEX", "INDEX");
            }
            String schema = obj.getSchema();
            if(!schemaMap.containsKey(schema)){
                continue;
            }
            String ts = schemaMap.get(schema);
            sql1=sql1.replace("${SCHEMA_NAME}",ts);
            obj.setSql(sql1);
            objres.add(obj);
        }
        objres.forEach(ob-> System.out.println(ob.getSql()));
        return objres;
    }










}

