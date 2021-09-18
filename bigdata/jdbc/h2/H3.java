package com.chail.apputil.jdbc.h2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.chail.apputil.jdbc.jdbcutilsone.JDBCUtil;
import com.chail.apputil.jdbc.jdbcutilsone.JdbcDirver;

public class H3 {

	private static int i = 0;
	private static PreparedStatement pstm = null;
	private static int commitsize = 10000;
	private static Connection connection;

	public static void main(String[] args) throws Exception {

		String url = "jdbc:h2:tcp://192.168.40.2:9093/mem:h2db";
		JDBCUtil jdbcUtil = new JDBCUtil(url, "org.h2.Driver", "sa", "");
		connection = jdbcUtil.getConnection();
		String sql = "INSERT INTO test3 (ID, NAME, SFZID, MOBILE, CARDNO, EMAIL, ADDRESS, POSTAL, CVV, IP, ZZJGDM, ZZJGMC, BIRTHDAY, MONEY, STRING, NUM, YLJGDJH, YSZGZS, YSZYZS, YYZZ, SHTYXYDM, PASSPORT, SWDJZ, KHXKZ, JGZ, CHINAPASSPORT, GATXZ, JJZZY, TWTBDLTXZ, JJMC, JJDM)"
				+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		pstm = connection.prepareStatement(sql);

		for (int i = 0; i < 100000000; i++) {
			update(i, "沙琳家", "411722196602231000", "18857863129", "6225885201285279", "503370969@qq.com",
					"上海市杨浦区国和二村162号401室", "46100", "3840", "38.109.166.124", "466988575", "北京大学附近",
					"2001-03-04 21:10:37", "786868.15", "nukk", "asd", "76192838-065320011A5241",
					"200212342411722196602231000", "241340300006917", "410800652011701", "4113000046698857510",
					"B1X6WQ3E", "150200466988575", "L5676777714901", "成字第4357527号", "E55828955", "C0942895004",
					"ZMT375448050471", "T45710403", "ss", "aa");
		}

	}

	private static void update(Object... params) {

		int index = 1;
		try {

			if (params != null && params.length > 0) {
				for (int i = 0; i < params.length; i++) {
					pstm.setObject(index++, params[i]);
				}
			}
			pstm.addBatch();
			i++;
			if (i % commitsize == 0) {
				pstm.executeBatch();
				connection.commit();
				pstm.clearBatch();
				System.out.println("commit " + i);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
