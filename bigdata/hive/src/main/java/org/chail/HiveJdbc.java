package org.chail;

import org.apache.hadoop.security.UserGroupInformation;


import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName : HiveJdbc
 * @Description :
 * @Author : Chail
 * @Date: 2020-11-25 16:46
 */
public class HiveJdbc {
    private static final String CONFIG_PATH = System.getProperty("user.dir")+File.separator+ "hive"+File.separator +"config";
    /**
     * 用于连接Hive所需的一些参数设置 driverName:用于连接hive的JDBC驱动名 When connecting to
     * HiveServer2 with Kerberos authentication, the URL format is:
     * jdbc:hive2://<host>:<port>/<db>;principal=
     * <Server_Principal_of_HiveServer2>
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    /**
     * 注意：这里的principal是固定不变的，其指的hive服务所对应的principal,
     */
    private static String url = "jdbc:hive2://172.16.67.3:21066/default;principal=hive/hadoop.hadoop.com@HADOOP.COM";
    private static String sql = "";
    private static ResultSet res;
    private static String principalNames = "huatu@HADOOP.COM";
    public static Connection get_conn() throws SQLException, ClassNotFoundException, IOException {
        String ss = CONFIG_PATH + File.separator + "krb5.conf";
        System.setProperty("java.security.krb5.conf", ss);
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }

    /**
     * 查看数据库下所有的表
     *
     * @param statement
     * @return
     */
    public static boolean show_tables(Statement statement) {
        sql = "SHOW TABLES";
        System.out.println("Running:" + sql);
        try {
            ResultSet res = statement.executeQuery(sql);
            System.out.println("执行“+sql+运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 查看数据库下所有的表
     *
     * @param statement
     * @return
     */
    public static boolean show_databases(Statement statement) {
        sql = "SHOW DATABASES";
        System.out.println("Running:" + sql);
        try {
            ResultSet res = statement.executeQuery(sql);
            System.out.println("执行“+sql+运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取表的描述信息
     *
     * @param statement
     * @param tableName
     * @return
     */
    public static boolean describ_table(Statement statement, String tableName) {
        sql = "DESCRIBE " + tableName;
        try {
            res = statement.executeQuery(sql);
            System.out.print(tableName + "描述信息:");
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除表
     *
     * @param statement
     * @param tableName
     * @return
     */
    public static boolean drop_table(Statement statement, String tableName) {
        sql = "DROP TABLE IF EXISTS " + tableName;
        System.out.println("Running:" + sql);
        try {
            statement.execute(sql);
            System.out.println(tableName + "删除成功");
            return true;
        } catch (SQLException e) {
            System.out.println(tableName + "删除失败");
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 查看表数据
     *
     * @param statement
     * @return
     */
    public static boolean queryData(Statement statement, String tableName) {
        sql = "SELECT * FROM " + tableName + " LIMIT 20";
        System.out.println("Running:" + sql);
        try {
            res = statement.executeQuery(sql);
            System.out.println("执行“+sql+运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1) + "," + res.getString(2) + "," + res.getString(3));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建表
     *
     * @param statement
     * @return
     */
    public static boolean createTable(Statement statement, String tableName) {
        //  为了方便直接复制另一张表数据来创建表
        sql = "CREATE TABLE "+tableName+" (aa int ,bb string)";
        System.out.println("Running:" + sql);
        try {
            boolean execute = statement.execute(sql);
            System.out.println("执行结果 ：" + execute);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        try {
            Connection conn = get_conn();
            System.out.println("连接成功");
            Statement stmt = conn.createStatement();
            // 创建的表名
            String tableName = "test_100m";
            show_databases(stmt);

            show_tables(stmt);

            // describ_table(stmt, tableName);
            /** 删除表 **/
             //drop_table(stmt, tableName);

            //show_tables(stmt);
            // queryData(stmt, tableName);
            //createTable(stmt, tableName);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("!!!!!!END!!!!!!!!");
        }
    }






}
