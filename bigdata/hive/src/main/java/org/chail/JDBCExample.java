package org.chail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class JDBCExample {
  private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
  
  private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
  private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";    
  private static String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = null;
  
  private static Configuration CONF = null; 
  private static String KRB5_FILE = null;
  private static String USER_NAME = null;
  private static String USER_KEYTAB_FILE = null;
  
  private static String zkQuorum = null;//zookeeper节点ip和端口列表
  private static String auth = null;
  private static String sasl_qop = null;
  private static String zooKeeperNamespace = null;
  private static String serviceDiscoveryMode = null;
  private static String principal = null;
  private static String AUTH_HOST_NAME = null;

  public static String getUserRealm() {
    String server_Realm = System.getProperty("SERVER_REALM");
    if (server_Realm != null && server_Realm != "") {
      AUTH_HOST_NAME = "hadoop." + server_Realm.toLowerCase();
    } else {
      server_Realm = KerberosUtil.getKrb5DomainRealm();
      if(server_Realm != null && server_Realm != "") {
        AUTH_HOST_NAME = "hadoop." + server_Realm.toLowerCase();
      } else {
        AUTH_HOST_NAME = "hadoop";
      }
    }
    return AUTH_HOST_NAME;
  }

  private static void init() throws IOException{
    CONF = new Configuration();

    Properties clientInfo = null;
    String userdir = System.getProperty("user.dir") + File.separator+"hive"+ File.separator
        + "config" + File.separator;
    InputStream fileInputStream = null;
    try{
      clientInfo = new Properties();
      //"hiveclient.properties"为客户端配置文件，如果使用多实例特性，需要把该文件换成对应实例客户端下的"hiveclient.properties"
      //"hiveclient.properties"文件位置在对应实例客户端安裝包解压目录下的config目录下
      String hiveclientProp = userdir + "hiveclient.properties" ;
      File propertiesFile = new File(hiveclientProp);
      fileInputStream = new FileInputStream(propertiesFile);
      clientInfo.load(fileInputStream);
    }catch (Exception e) {
      throw new IOException(e);
    }finally{
      if(fileInputStream != null){
        fileInputStream.close();
        fileInputStream = null;
      }
    }
    //zkQuorum获取后的格式为"xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002";
    //"xxx.xxx.xxx.xxx"为集群中ZooKeeper所在节点的业务IP，端口默认是24002
    zkQuorum =  clientInfo.getProperty("zk.quorum");
    auth = clientInfo.getProperty("auth");
    sasl_qop = clientInfo.getProperty("sasl.qop");
    zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
    serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");
    principal = clientInfo.getProperty("principal"); 
    // 设置新建用户的USER_NAME，其中"xxx"指代之前创建的用户名，例如创建的用户为user，则USER_NAME为user
    USER_NAME = "chail@HADOOP.COM";

    if ("KERBEROS".equalsIgnoreCase(auth)) {
      // 设置客户端的keytab和krb5文件路径
      USER_KEYTAB_FILE = "hive/config/user.keytab";
      KRB5_FILE = userdir + "krb5.conf";
      System.setProperty("java.security.krb5.conf", KRB5_FILE);
      ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/" + getUserRealm();
      System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
    }

    String ss = userdir + "krb5.conf";
    String principalNames = "chail@HADOOP.COM";
    System.setProperty("java.security.krb5.conf", ss);
    /** 使用Hadoop安全登录 **/
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set("hadoop.security.authentication", "Kerberos");

    try {
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(principalNames, userdir+ "user.keytab");
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }
  
  /**
   * 本示例演示了如何使用Hive JDBC接口来执行HQL命令<br>
   * <br>
   * 
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws SQLException
   * @throws IOException 
   */
  public static void main(String[] args) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException, SQLException, IOException{
    // 参数初始化
    init();

    // 定义HQL，HQL为单条语句，不能包含“;”
    String[] sqls = {"CREATE TABLE IF NOT EXISTS employees_info(id INT,name STRING)",
        "SELECT COUNT(*) FROM employees_info", "DROP TABLE employees_info"};

    // 拼接JDBC URL
    StringBuilder sBuilder = new StringBuilder(
        "jdbc:hive2://").append(zkQuorum).append("/");

    if ("KERBEROS".equalsIgnoreCase(auth)) {
      sBuilder.append(";serviceDiscoveryMode=") 
              .append(serviceDiscoveryMode)
              .append(";zooKeeperNamespace=")
              .append(zooKeeperNamespace)
              .append(";sasl.qop=")
              .append(sasl_qop)
              .append(";auth=")
              .append(auth)
              .append(";principal=")
              .append(principal)
              .append(";user.principal=")
              .append(USER_NAME)
              .append(";user.keytab=")
              .append(USER_KEYTAB_FILE)
              .append(";");
    } else {
	    //普通模式
      sBuilder.append(";serviceDiscoveryMode=") 
              .append(serviceDiscoveryMode)
              .append(";zooKeeperNamespace=")
              .append(zooKeeperNamespace)
              .append(";auth=none");
    }
    String url = sBuilder.toString();
    System.out.println(url);
    // 加载Hive JDBC驱动
    Class.forName(HIVE_DRIVER);

    Connection connection = null;
    try {
      // 获取JDBC连接
      // 如果使用的是普通模式，那么第二个参数需要填写正确的用户名，否则会以匿名用户(anonymous)登录
      connection = DriverManager.getConnection(url, "", "");
        
      // 建表
      // 表建完之后，如果要往表中导数据，可以使用LOAD语句将数据导入表中，比如从HDFS上将数据导入表:
      // load data inpath '/tmp/employees.txt' overwrite into table employees_info;
      execDDL(connection,sqls[0]);
      System.out.println("Create table success!");
       
      // 查询
      execDML(connection,sqls[1]);
        
      // 删表
      execDDL(connection,sqls[2]);
      System.out.println("Delete table success!");
    }catch (Exception e) {
      System.out.println("Create connection failed : "  + e.getMessage());
    }
    finally {
      // 关闭JDBC连接
      if (null != connection) {
        connection.close();
      }
    }
  }
  
  public static void execDDL(Connection connection, String sql)
  throws SQLException {
    PreparedStatement statement = null;
    try {
      statement = connection.prepareStatement(sql);
      statement.execute();
    }
    finally {
      if (null != statement) {
        statement.close();
      }
    }
  }


  public static void execDML(Connection connection, String sql) throws SQLException {
    PreparedStatement statement = null;
    ResultSet resultSet = null;
    ResultSetMetaData resultMetaData = null;
    
    try {
      // 执行HQL
      statement = connection.prepareStatement(sql);
      resultSet = statement.executeQuery();
      
      // 输出查询的列名到控制台
      resultMetaData = resultSet.getMetaData();
      int columnCount = resultMetaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        System.out.print(resultMetaData.getColumnLabel(i) + '\t');
      }
      System.out.println();
      
      // 输出查询结果到控制台
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; i++) {
          System.out.print(resultSet.getString(i) + '\t');
        }
        System.out.println();
      }
    }
    finally {
      if (null != resultSet) {
        resultSet.close();
      }
      
      if (null != statement) {
        statement.close();
      }
    }
  }

}
