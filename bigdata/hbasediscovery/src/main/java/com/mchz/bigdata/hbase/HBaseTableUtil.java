package com.mchz.bigdata.hbase;

import java.util.List;
import java.util.stream.Collectors;

public class HBaseTableUtil {
	public static String toSchemaSQL(HBaseTable hbaseTable) {
		return String.format("CREATE SCHEMA IF NOT EXISTS %s", hbaseTable.getNamespace());
	}
	
	public static String toSQL(HBaseTable hbaseTable) {
		List<HBaseColumn> columns = hbaseTable.getHbaseColumnList();
		String x = columns.stream().map(c -> c.toSQL()).collect(Collectors.joining(",\n  "));
		return String.format("CREATE TABLE %s (\n  %s)", hbaseTable.getNameSQL(), x);
	}
	
	public static String toSQLInsertSyntax(HBaseTable hbaseTable) {
		List<HBaseColumn> columns = hbaseTable.getHbaseColumnList();
		String names = columns.stream().map(c -> c.toSQLName()).collect(Collectors.joining(", "));
		String binds = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
		return String.format("INSERT INTO %s (%s) VALUES (%s)", hbaseTable.getNameSQL(), names, binds);
	}
}
