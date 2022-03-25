package com.chail;

import net.sourceforge.jtds.jdbc.TypeInfo;

import java.sql.*;

/**
 * @Author: yangc
 * @Date: 2022/1/13 9:43
 */
public class MssqlTimebug {

    //public static String url_encrypt = "jdbc:jtds:sqlserver://192.168.225.30:1434/master";

    public static String url_encrypt = "jdbc:sqlserver://192.168.225.30:1434";


    public static String user = "sa";
    public static String password = "Hzmc321#";

    public static void main(String[] args) throws Exception {
        //Class.forName("net.sourceforge.jtds.jdbc.Driver");

        //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        //建立数据库对象
        Connection conn = DriverManager.getConnection(url_encrypt, user, password);
        //建立操作对象
        Statement stmt = conn.createStatement();
        //结果集
        ResultSet rs = stmt.executeQuery("select * from dbo.test_date");
        ResultSetMetaData metaData = rs.getMetaData();
        int columnType = metaData.getColumnType(1);
        //依次输出结果集内容
        while (rs.next()) {
            Object object = rs.getObject(1);
            Object object2 = rs.getObject(2);
            System.out.println();
        }
        //依次关闭结果集，操作对象，数据库对象
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}

