package org.chail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveKerberos {


    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        //System.setProperty("sun.security.krb5.debug","true");
        Configuration entries = kerberosLogin();
        hiveConnect();
        Path path = new Path("hdfs://192.168.20.104:4007");
        FileSystem fileSystem = FileSystem.get(path.toUri(),entries);
        System.out.println(fileSystem.getScheme());
        long l = System.currentTimeMillis();
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("hdfs://192.168.20.104:4007/"));
        System.out.println(fileStatus.toString());
        System.out.println(System.currentTimeMillis() - l);

    }


    public static Configuration kerberosLogin() throws IOException {
        String CONFIG_PATH = System.getProperty("user.dir") + File.separator+"bigdata"+File.separator + "hive" + File.separator + "hive-config";
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "kerberos");
        String krb5FilePath = CONFIG_PATH + File.separator + "krb5.conf";
        String kerberosKeytabPath = CONFIG_PATH + File.separator + "emr.keytab";
        System.setProperty("java.security.krb5.conf", krb5FilePath);
        UserGroupInformation.setConfiguration(configuration);
                UserGroupInformation.loginUserFromKeytab(
                        "hadoop/192.168.0.8@EMR-62RP2PNP", kerberosKeytabPath);
                return configuration;
    }


    public static void hiveConnect() throws ClassNotFoundException, SQLException {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        /**
         * 注意：这里的principal是固定不变的，其指的hive服务所对应的principal,
         */
        String url = "jdbc:hive2://192.168.20.104:7001/default;principal=hadoop/192.168.0.8@EMR-62RP2PNP";
        Class.forName(driverName);
        long l = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url);
        System.out.println(System.currentTimeMillis() - l);
        ResultSet resultSet = conn.createStatement().executeQuery("show databases");

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }
}
