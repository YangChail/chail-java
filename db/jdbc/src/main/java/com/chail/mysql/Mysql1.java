package com.chail.mysql;

import com.chail.oracle.JDBCUtil;
import com.chail.oracle.JdbcDirver;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.sql.*;


public class Mysql1 {

	
	
	public static void main(String[] args) throws SQLException {
		String hostname="10.11.128.244";

		JSch jsch = new JSch();
		Session session = null;
		try {
			String host="wz.ssh";
			String user="dm";
			String password="dm";
			session = jsch.getSession(user, host,1205 );
			session.setPassword(password);
			session.setConfig("StrictHostKeyChecking", "no");
			// step1：建立ssh连接
			session.connect();
			System.out.println(session.getServerVersion());//这里打印SSH服务器版本信息
			//step2： 设置SSH本地端口转发，本地转发到远程
			int assinged_port = session.setPortForwardingL(19003, hostname, 3306);
			session.
			System.out.println("ssh隧道配置： localhost:" + assinged_port + " -> " + hostname + ":" + 3306);
		} catch (Exception e) {
			if (null != session) {
				//关闭ssh连接
				session.disconnect();
			}
			e.printStackTrace();
		}

		String parm="serverTimezone=Asia/Shanghai";
		String user1 = "dmcp";
		String pass = "Dmcp321!";
		String url = "jdbc:mysql://localhost:19003/mc_center";
//		String user = "root";
//		String pass = "hzmc321#";
//		String url = "jdbc:mysql://192.168.50.24:3306/test";
		JDBCUtil jdbcUtil = new JDBCUtil(url, JdbcDirver.MYSQL_DRIVER, user1, pass);
		Connection connection = jdbcUtil.getConnection();
		//inster(connection);
		//update(connection);
		//create(connection);
		String sql="select * from flyway_schema_history";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		ResultSet resultSet = preparedStatement.executeQuery();


		while (resultSet.next()){
			Object object = resultSet.getObject(1);
			System.out.println(object);
		}

	}
		private static void update(Connection connection) throws SQLException {
		String sql="select * from flyway_schema_history";
		//String sql1="select count(*) from test1";
		connection.setAutoCommit(false);
		PreparedStatement prepareStatement = connection.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		ResultSet executeQuery = prepareStatement.executeQuery();
		while (executeQuery.next()) {
			executeQuery.updateString(2, "upsssss-");
			executeQuery.updateRow();
			//prepareStatement.executeUpdate();
		}
		connection.commit();

		
		
	}
		
		
		
		private static void select(Connection connection) {
			String sql="SELECT \r\n" + 
					"c.TABLE_SCHEMA,\r\n" + 
					"c.TABLE_NAME,\r\n" + 
					"c.COLUMN_NAME,\r\n" + 
					"c.DATA_TYPE,\r\n" + 
					"c.IS_NULLABLE,\r\n" + 
					"c.COLUMN_COMMENT,\r\n" + 
					"c.ORDINAL_POSITION,\r\n" + 
					"c.EXTRA,\r\n" + 
					"c.COLUMN_DEFAULT,\r\n" + 
					"(case when c.DATA_TYPE = 'float' or c.DATA_TYPE = 'double' or c.DATA_TYPE = 'decimal' then c.NUMERIC_PRECISION else c.CHARACTER_MAXIMUM_LENGTH end ) as length,\r\n" + 
					"c.NUMERIC_SCALE\r\n" + 
					" FROM information_schema.COLUMNS c inner join information_schema.TABLES t \r\n" + 
					"WHERE \r\n" + 
					" t.TABLE_TYPE<>'VIEW' AND c.TABLE_NAME=t.TABLE_NAME AND c.TABLE_SCHEMA=t.TABLE_SCHEMA ORDER BY 1, 2, 7";
			
			
			
			
			
			
		}
		
		private static void create(Connection connection) throws SQLException {
			String sql="CREATE TABLE `chail`.`incress1s1` (`id` int NOT NULL ,`column1` varchar ,`datecol` date ) ";
			Statement prepareStatement = connection.createStatement();
			prepareStatement.execute(sql);
		}
		
		
	
	
	
	private static void inster(Connection connection) throws SQLException {
		PreparedStatement prepareStatement = connection.prepareStatement("insert into test2(id,col1,col2) values(?,?,?)");
		connection.setAutoCommit(false);
		for(int i=1;i<100;i++) {
			prepareStatement.setInt(1, i);
			prepareStatement.setString(2, String.valueOf(i));
			prepareStatement.setString(3, String.valueOf(i));
			prepareStatement.executeUpdate();
		}
		connection.commit();
		
	}
}
