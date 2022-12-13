package com.chail.oracle;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @author : yangc
 * @date :2022/12/9 15:30
 * @description :
 * @modyified By:
 */
public class GetSysdate {


    public static void main(String[] args) throws Exception {
        String user = "dbrep";
        String pass = "dbrep123";
        String url = "jdbc:oracle:thin:@192.26.22.86:1521/hisdb";
        System.out.println(url);
        JDBCUtil jdbcUtil = new JDBCUtil(url, JdbcDirver.ORACLE_DRIVER, user, pass);
        Connection connection =null;
        try {
             connection = jdbcUtil.getConnection();
            String sql="select sysdate from dual";
            List<Map<String, Object>> maps = jdbcUtil.executeQuery(sql);
            for (Map<String, Object> map : maps) {
                map.forEach((k,v)->{
                    System.out.println(k+"  "+v);
                });
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            connection.close();
        }

        String user1 = "etl_source";
        String pass1 = "etl_source";
        String url1 = "jdbc:oracle:thin:@192.26.244.238:1521/orcl";
        System.out.println(url1);
        JDBCUtil jDBCUtil1 = new JDBCUtil(url1, JdbcDirver.ORACLE_DRIVER, user1, pass1);
        Connection connection1 =null;
        try {
            connection1 = jDBCUtil1.getConnection();
            String sql="select sysdate from dual";
            List<Map<String, Object>> maps = jDBCUtil1.executeQuery(sql);
            for (Map<String, Object> map : maps) {
                map.forEach((k,v)->{
                    System.out.println(k+"  "+v);
                });
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            connection1.close();
        }


    }

}
