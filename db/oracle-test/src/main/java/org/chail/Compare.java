package org.chail;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Compare {

   private static final String DB_URL = "jdbc:postgresql://127.0.0.1:45432/dataflow";
   // private static final String DB_URL = "jdbc:postgresql://192.168.170.129:45432/dataflow";
    private static final String DB_USER = "dataflow";
    private static final String DB_PASSWORD = "Hzmc321#";

    private static final String sql="CREATE TABLE mc_compare_table_filter (\n" +
            "\tjob_id serial4 NOT NULL,\n" +
            "\ttable_id bigserial NOT NULL\n" +
            ");";

    private static final String sqlTable="select id FROM mc_sensitive_table where sensitive_source_id =%s and schema_name='%s' and name='%s' ";

    private static final String sqlInsert="insert into mc_compare_table_filter values (%s,%s);";

    private static final String deleteSql="delete from mc_compare_table_filter where job_id =%s ";

    private static  int jobId=3;

    private static  int sourceId=3;
    private static  Connection connection =null;

    public static void createtable(boolean isBig) throws SQLException {
         try {
             if(isBig){
                 jobId=2;
                 sourceId=2;
             }
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            SqlUtils.execute(connection,sql);
            delete();
        } catch (Exception e) {
            System.out.println("忽略建表失败");
        }
    }

    public static void delete() throws SQLException {
        try {
            SqlUtils.execute(connection,String.format(deleteSql,jobId));
        } catch (Exception e) {
            System.out.println("忽略建表失败");
        }
    }



    public static int getTableId(String schema,String table){
        try {
            Object one = SqlUtils.getOne(connection, String.format(sqlTable, sourceId,schema, table));
            if(one!=null){
                return Integer.parseInt(one.toString());
            }
        } catch (NumberFormatException e) {
            System.out.println("获取表格失败");
        }
        return -1;
    }


    public static void insertTable(int tableId){
        try {
            SqlUtils.execute(connection,String.format(sqlInsert,jobId,tableId ));
        } catch (NumberFormatException e) {
            System.out.println("插入失败");
        }

    }

}
