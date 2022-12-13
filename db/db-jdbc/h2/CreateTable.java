package com.chail.apputil.jdbc.h2;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.chail.apputil.jdbc.jdbcutilsone.JDBCUtil;

public class CreateTable {

	public static void main(String[] args) throws Exception {

	}

	private static void createTable() {
		String url = "jdbc:h2:tcp://192.168.40.2:9093/mem:h2db";
		JDBCUtil jdbcUtil = new JDBCUtil(url, "org.h2.Driver", "sa", "1234");
		Connection connection = jdbcUtil.getConnection();
		List<String> create = create();
		for (String str : create) {
			jdbcUtil.executeUpdate(str);
		}

	}

	private static List<String> create() {
		List<String> table = new ArrayList<String>();
		for (int i = 1; i <= 20; i++) {
			String sql = "CREATE TABLE PUBLIC.TABLE" + i + " (\r\n" + "	ID VARCHAR(100),\r\n"
					+ "	NAME VARCHAR(100),\r\n" + "	IDCARD VARCHAR(100),\r\n" + "	PHONE VARCHAR(100),\r\n"
					+ "	BANKCARD VARCHAR(100),\r\n" + "	ORGNUM VARCHAR(100),\r\n" + "	COL1 VARCHAR(100),\r\n"
					+ "	COL2 VARCHAR(100),\r\n" + "	COL3 VARCHAR(100),\r\n" + "	COL4 VARCHAR(100),\r\n"
					+ "	COL5 VARCHAR(100),\r\n" + "	COL6 VARCHAR(100),\r\n" + "	COL7 VARCHAR(100),\r\n"
					+ "	COL8 VARCHAR(100),\r\n" + "	COL9 VARCHAR(100),\r\n" + "	COL10 VARCHAR(100),\r\n"
					+ "	COL11 VARCHAR(100),\r\n" + "	COL12 VARCHAR(100),\r\n" + "	COL13 VARCHAR(100),\r\n"
					+ "	COL14 VARCHAR(100),\r\n" + "	COL15 VARCHAR(100)\r\n" + ")";
			table.add(sql);
		}
		return table;
	}

}
