package com.chail.h2;

import com.chail.oracle.JDBCUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class H4 {
	public static String[] TABLE = new String[] { "table1", "table2", "table3", "table4", "table5", "table6", "table7", "table8",
			"table9", "table10", "table11", "table12", "table13", "table14", "table15", "table16", "table17", "table18", "table19" , "table20"};
	private static String tableName="table";
	private static int  table_num=20;
	private static int data_num = 500000;
	private static String url = "jdbc:h2:tcp://192.168.51.123:9093/mem:h2db;MULTI_THREADED=1;DB_CLOSE_DELAY=-1";

	public static void main(String[] args) throws Exception {
		 dropTable();
		createTable();
		insert();
	}

	private static void insert() throws SQLException, InterruptedException {
		ThreadPoolExecutor ex = new ThreadPoolExecutor(20, 20, 20L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
		for (String str:TABLE) {
			ex.execute(() -> {
				try {
					insert(str);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			});

		}

		ex.shutdown();

		while (!ex.awaitTermination(5, TimeUnit.SECONDS)) {

		}

	}

	private static void insert(String tableName) throws SQLException {
		JDBCUtil jdbcUtil = new JDBCUtil(url, "org.h2.Driver", "sa", "1234");
		Connection connection = jdbcUtil.getConnection();
		String sql = "INSERT INTO " + tableName
				+ "(ID, NAME, IDCARD, PHONE, BANKCARD, ORGNUM, COL1, COL2, COL3, COL4, COL5, COL6, COL7, COL8, COL9, COL10, COL11, COL12, COL13, COL14)\r\n"
				+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement pstm = connection.prepareStatement(sql);
		String byte1 = getByte(100);
		for (int i = 0; i < data_num; i++) {
			update(connection, pstm, i, i, "张三丰", "411722196602231000", "18857863129", "6228482410842133810",
					"466988575", byte1, byte1, byte1, byte1, byte1, byte1, byte1, byte1, byte1, byte1, byte1, byte1,
					byte1, byte1);

		}
		pstm.executeBatch();
		jdbcUtil.releaseConnectn();
	}

	private static String getByte(int num) {
		byte[] b = new byte[num];
		Arrays.fill(b, (byte) 0x4e);
		String s = new String(b);
		return s;
	}

	private static void update(Connection connection, PreparedStatement pstm, int i, Object... params) {

		int index = 1;
		try {

			if (params != null && params.length > 0) {
				for (int j = 0; j < params.length; j++) {
					pstm.setObject(index++, params[j]);
				}
			}
			pstm.addBatch();
			i++;
			if (i % 10000 == 0) {
				pstm.executeBatch();
				connection.commit();
				pstm.clearBatch();
				System.out.println(Thread.currentThread().getName() + "  commit " + i);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static void createTable() {
		
		JDBCUtil jdbcUtil = new JDBCUtil(url, "org.h2.Driver", "sa", "1234");
		jdbcUtil.getConnection();
		List<String> create = create();
		for (String str : create) {
			jdbcUtil.executeUpdate(str);
		}

		jdbcUtil.releaseConnectn();

	}

	private static List<String> create() {
		List<String> table = new ArrayList<String>();
		for (int i = 1; i <= table_num; i++) {
			String sql = "CREATE TABLE PUBLIC.TABLE" + i + " (\r\n" + "	ID VARCHAR(100),\r\n"
					+ "	NAME VARCHAR(100),\r\n" + "	IDCARD VARCHAR(100),\r\n" + "	PHONE VARCHAR(100),\r\n"
					+ "	BANKCARD VARCHAR(100),\r\n" + "	ORGNUM VARCHAR(100),\r\n" + "	COL1 VARCHAR(100),\r\n"
					+ "	COL2 VARCHAR(100),\r\n" + "	COL3 VARCHAR(100),\r\n" + "	COL4 VARCHAR(100),\r\n"
					+ "	COL5 VARCHAR(100),\r\n" + "	COL6 VARCHAR(100),\r\n" + "	COL7 VARCHAR(100),\r\n"
					+ "	COL8 VARCHAR(100),\r\n" + "	COL9 VARCHAR(100),\r\n" + "	COL10 VARCHAR(100),\r\n"
					+ "	COL11 VARCHAR(100),\r\n" + "	COL12 VARCHAR(100),\r\n" + "	COL13 VARCHAR(100),\r\n"
					+ "	COL14 VARCHAR(100),\r\n"  + ")";
			table.add(sql);
		}
		return table;
	}
	
	private static void dropTable() {
		
		JDBCUtil jdbcUtil = new JDBCUtil(url, "org.h2.Driver", "sa", "1234");
		jdbcUtil.getConnection();
		for (int i = 1; i <= table_num; i++) {
			String str=tableName+i;
			jdbcUtil.executeUpdate("drop table if exists "+str);
		}

		jdbcUtil.releaseConnectn();

	}
}
