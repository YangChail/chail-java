package com.mchz.bigdata.hdfs.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

import java.io.File;
import java.io.IOException;

public class TestMain {
  private final static Log LOG = LogFactory.getLog(TestMain.class.getName());

  private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
  private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
  private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

  private static Configuration conf = null;
  private static String krb5File = null;
  private static String userName = null;
  private static String userKeytabFile = null;

  public static void main(String[] args) throws IOException {
    try {
      init();
      login();
    } catch (IOException e) {
      LOG.error("Failed to login because ", e);
      return;
    }
    Connection connection = ConnectionFactory.createConnection(conf);
    HTableDescriptor[] hTableDescriptors = connection.getAdmin().listTables();

    HTableDescriptor[] hTableDescriptors1 = connection.getAdmin().listTables();

  }

  private static void login() throws IOException {
    if (User.isHBaseSecurityEnabled(conf)) {
      String userdir = System.getProperty("user.dir") + File.separator + "config" + File.separator;
      userName = "admin@HADOOP.COM";
      userKeytabFile = userdir + "user.keytab";
      krb5File = userdir + "krb5.conf";

      /*
       * if need to connect zk, please provide jaas info about zk. of course,
       * you can do it as below:
       * System.setProperty("java.security.auth.login.config", confDirPath +
       * "jaas.conf"); but the demo can help you more : Note: if this process
       * will connect more than one zk cluster, the demo may be not proper. you
       * can contact us for more help
       */
      LoginUtil.setJaasConf("Client", userName, userKeytabFile);
      LoginUtil.login(userName, userKeytabFile, krb5File, conf);
    }
  }

  private static void init() throws IOException {
    // Default load from conf directory
    conf = HBaseConfiguration.create();
    String userdir = System.getProperty("user.dir") + File.separator + "config" + File.separator;
    conf.addResource(new Path(userdir + "core-site.xml"), false);
    conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
    conf.addResource(new Path(userdir + "hbase-site.xml"), false);

  }

}
