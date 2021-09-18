package com.mchz.bigdata.hbase;

import org.apache.hadoop.hbase.client.Result;

import java.sql.PreparedStatement;

public class TableRowUtil {
	
	public static void populateStatement(PreparedStatement s, Result result, HBaseTable hbaseTable) throws Exception {
		Object[] row = HBaseValueUtil.getValue(result, hbaseTable);
		for (int i = 0; i < row.length; i++) {
			s.setObject(i + 1, row[i]);
		}
	}

}
