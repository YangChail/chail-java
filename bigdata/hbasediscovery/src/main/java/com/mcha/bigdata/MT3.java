package com.mcha.bigdata;

import java.sql.*;

/**
 * @ClassName : MT3
 * @Description :
 * @Author : Chail
 * @Date: 2020-11-23 18:54
 */
public class MT3 {

    public static Connection getConn() {
        Connection conn = null;
        try {

            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://192.168.51.203:20051/metadata?use_boolean=true";
            try {
                conn = DriverManager.getConnection(url, "postgres", "postgres");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void main(String[] args) {

        Connection conn = getConn();
        String sql = "show tables";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.println(rs.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
