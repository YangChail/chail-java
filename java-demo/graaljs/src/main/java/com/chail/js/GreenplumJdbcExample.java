package com.chail.js;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GreenplumJdbcExample {
    
    private static final String JDBC_DRIVER = "org.postgresql.Driver";
    private static final String DB_URL = "jdbc:postgresql://192.168.42.210:2345/sc_sc";
    private static final String USER = "sc_sc";
    private static final String PASSWORD = "sc_sc";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开连接
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);

            // 执行查询
            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql = "SELECT\n" +
                    "\tDISTINCT C.SCHEMANAME AS schemas,\n" +
                    "\tC.TABLENAME,\n" +
                    "\tC.INDEXNAME,\n" +
                    "\tC.INDEXDEF\n" +
                    "FROM\n" +
                    "\tPG_CATALOG.PG_INDEX A,\n" +
                    "\tPG_CATALOG.PG_CLASS B ,\n" +
                    "\tPG_CATALOG.PG_INDEXES C\n" +
                    "WHERE\n" +
                    "\tA.INDEXRELID = B.OID\n" +
                    "\tAND B.RELNAME = C.INDEXNAME\n" +
                    "\tAND A.INDISUNIQUE = 'FALSE'\n" +
                    "\tAND A.INDISPRIMARY = 'FALSE'\n" +
                    "\tand C.INDEXNAME like 'MC_%'\n" +
                    "\torder by schemas,TABLENAME";
            rs = stmt.executeQuery(sql);
            Set<String> cash=new HashSet<>();
            List<String> aa=new ArrayList<>();
            List<String> res=new ArrayList<>();
            List<String> sqls=new ArrayList<>();
            // 处理查询结果
            while (rs.next()) {
                String sqli = rs.getString(4);
                int i = sqli.indexOf("USING btree");
                if(i>1){
                    sqli = sqli.substring(i, sqli.length());
                }

                String index_name = rs.getString(3);
                String schema = rs.getString(1);
                String table = rs.getString(2);
                if(cash.contains(schema+"."+table+"."+sqli)){
                    res.add("\""+schema+"\""+".\""+index_name+"\"");
                    sqls.add(sqli);
                }else{
                    aa.add(sqli);
                }
                cash.add(schema+"."+table+"."+sqli);
            }



            for (String re : res) {
                String dropsql=String.format(" DROP INDEX %s;",re);
                System.out.println(dropsql);

            }

            System.out.println();

        } catch (SQLException se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        } catch (Exception e) {
            // 处理 Class.forName 错误
            e.printStackTrace();
        } finally {
            // 关闭资源
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        System.out.println("Query completed.");
    }



}