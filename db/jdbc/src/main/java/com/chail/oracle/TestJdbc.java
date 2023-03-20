package com.chail.oracle;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestJdbc {


	public static JDBCUtil testOracle9i() throws Exception {
		String user = "system";
		String pass = "oracle";
		String url = "jdbc:oracle:thin:@192.168.42.81:1521/orcl";
		JDBCUtil jdbcUtil = new JDBCUtil(url, JdbcDirver.ORACLE_DRIVER, user, pass);
		return jdbcUtil;
	}

	public static  void write() throws Exception {
		File file=new File("D://test.txt");
		MmapFile mmapFile=new MmapFile(file,4);
		JDBCUtil jdbcUtil = testOracle9i();
		Connection connection = jdbcUtil.getConnection();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("select *  FROM DQRMYY_W1.TEST_1");
		int i=0;
		while (resultSet.next()){
			String string = resultSet.getString(1);
			byte[] aByte=string.getBytes();
			mmapFile.write(aByte);
			if(i==0){
				System.out.println(string);
			}
			i++;
			if(i%10000==0){
				System.out.println(i);
			}
			if(i==100000){
				break;
			}
		}
		mmapFile.flush();
		mmapFile.close();
	}

	public static void read() throws Exception {
		File file=new File("D://test.txt");
		MmapFile mmapFile=new MmapFile(file,4);
		byte[] read = mmapFile.read(0, 2);
		String ss=new String(read);
		System.out.println();

	}


	public static void main(String[] args) throws Exception {
		write();
		read();
	}
}
