package com.chail.datasupport.tools;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.chail.datasupport.tools.model.JobConfig;
import com.chail.datasupport.tools.model.SsTable;
import com.chail.datasupport.tools.model.SqlObject;
import com.chail.datasupport.tools.model.V2SqView;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : yangc
 * @date :2022/6/9 10:16
 * @description :
 * @modyified By:
 */
public class Ds2 extends CheckJob {

    public static  DruidDataSource dataSource;



    public static synchronized void getConnection() throws ClassNotFoundException, SQLException {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://wz.chail.top:61038/dm");
        dataSource.setUsername("dm");
        dataSource.setPassword("hzmcdm");
        dataSource.setInitialSize(2);
        dataSource.setMaxActive(4);
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




    public static void updateConfig(V2SqView v2SqView) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        JobConfig jobConfig = getSql(v2SqView.getJobId());
        if(jobConfig==null||jobConfig.getId()==null){
            throw new SQLException("空数据");
        }
        List<SqlObject> sqlObjects = jobConfig.getSqlObjects();
        List<SsTable> tableId = getTableId(sqlObjects);
        String table = v2SqView.getTableName();
        String id = null;
        String sql = v2SqView.getSql();
        for (SsTable ssTable : tableId) {
            if (table.equals(ssTable.getName())) {
                id = ssTable.getSsSourceId();
                break;
            }
        }
        int i = 0;
        for (SqlObject sqlObject : sqlObjects) {
            List<Integer> sensitiveIds = sqlObject.getSensitiveIds();
            Integer integer = sensitiveIds.get(0);
            if (integer.toString().equals(id)) {
                String sql1 = sqlObject.getSql();
                System.out.println("=============");
                System.out.println("tableId:"+id);
                System.out.println("=============");
                System.out.println("  更新前:"+sql1);
                System.out.println("=============");
                System.out.println("  更新后:"+sql);
                sqlObject.setSql(sql);
                break;
            }
            i++;
        }
        String o = JSONObject.toJSON(sqlObjects).toString();
        jobConfig.setValue(o);
        update(jobConfig);
    }

    private static synchronized  void update(JobConfig jobConfig) {
        String sql = "update mc_job_config set \"value\"=? where id=?";
        //sql=String.format(sql,jobConfig.getValue(),jobConfig.getId());
        System.out.println(sql);
        //
        DruidPooledConnection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,jobConfig.getValue());
            preparedStatement.setObject(2,Long.valueOf(jobConfig.getId()));
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        CloseUtils.close(connection);

    }


    public static synchronized ResultSet getResult(DruidPooledConnection connection,String sql) throws SQLException {
        Statement stmt =  connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return rs;
    }

    public static JobConfig getSql(String jobId) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        String sql = "select * from mc_job_config  where job_id in ( select job_id from mc_job_config mjc where name = 'JOB_EXTERNAL_ID' and value = '%s' ) and name = 'MODEL_SOURCE_ID_DEPEND'";
        sql = String.format(sql, jobId);
        System.out.println(sql);
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


    public static List<SsTable> getTableId(List<SqlObject> sqlObjects) throws SQLException, IntrospectionException, InvocationTargetException, IllegalAccessException {
        StringBuffer sbf = new StringBuffer();
        for (SqlObject sqlObject : sqlObjects) {
            List<Integer> sourceIds = sqlObject.getSensitiveIds();
            Integer id = sourceIds.get(0);
            if (sbf.length() > 0) {
                sbf.append(",");
            }
            sbf.append(id);
        }
        String sql = "select * from mc_sensitive_table where sensitive_source_id in(%s)";
        sql = String.format(sql, sbf);
        System.out.println(sql);
        DruidPooledConnection connection = dataSource.getConnection();
        ResultSet rs = getResult(connection,sql);
        List<SsTable> list = new ArrayList<>();
        while (rs.next()) {
            SsTable ssTable = new SsTable();
            Db2ObjectUtils.getObj(rs, ssTable, SsTable.class);
            list.add(ssTable);
        }
        CloseUtils.close(rs, rs.getStatement(),connection);
        return list;
    }


}
