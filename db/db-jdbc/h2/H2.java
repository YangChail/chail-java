package com.chail.apputil.jdbc.h2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.chail.apputil.jdbc.jdbcutilsone.JDBCUtil;
import com.chail.apputil.jdbc.jdbcutilsone.JdbcDirver;

public class H2 {

	private static int i = 0;
	private static PreparedStatement pstm = null;
	private static int commitsize = 10000;
	private static Connection connection;

	public static void main(String[] args) throws Exception {
		System.out.println(getByte(100));
	}
	
	
	
	private static String getByte(int num) {
		byte[] b = new byte[num];
		Arrays.fill(b, (byte) 0x4e);
		String s = new String(b);
		return s;
	}
}