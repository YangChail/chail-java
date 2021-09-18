package com.chail.apputil.jdbc.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.chail.apputil.jdbc.jdbcutilsone.JDBCUtil;
import com.chail.apputil.jdbc.jdbcutilsone.JdbcDirver;

public class Mysql {

	
	
	public static void main(String[] args) throws SQLException {
		String parm="serverTimezone=Asia/Shanghai";
		String user = "root";
		String pass = "hzmc321#";
		String url = "jdbc:mysql://192.168.200.159:3306/yc";
//		String user = "root";
//		String pass = "hzmc321#";
//		String url = "jdbc:mysql://192.168.50.24:3306/test";
		JDBCUtil jdbcUtil = new JDBCUtil(url, JdbcDirver.MYSQL_DRIVER, user, pass);
		Connection connection = jdbcUtil.getConnection();
		//inster(connection);
		//update(connection);
		create(connection);
		
	}
		private static void update(Connection connection) throws SQLException {
		String sql="select * from test2";
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
