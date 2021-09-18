package com.mchz.bigdata.discovery;

import com.mchz.bigdata.hbase.HBaseConn;
import com.mchz.bigdata.hbase.HBaseTable;
import com.mchz.bigdata.hbase.HBaseUtil;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/*
 * 获取表清单
 * 获取表定义
 * 获取表数据
 * 
 * */

public class HBasePump {

	private Optional<List<?>> values;
	private List<HBaseTable> tables;
	private HBaseConn hbaseConn;
	private HBaseUtil hbaseUtil;

	public HBasePump(HBaseConn hbaseConn) {
		this.hbaseConn = hbaseConn;
		this.hbaseUtil = new HBaseUtil(hbaseConn);
		this.values = Optional.empty();
	}

	public List<HBaseTable> tables() throws Exception {
		this.tables = hbaseUtil.getAllTables();
		return this.tables;
	}

	public HBaseTable tableMatching(Predicate<HBaseTable> predicate) {
		Optional<HBaseTable> optionalHBaseTable = this.tables.stream()
				.filter(i -> predicate.test(i))
				.findFirst();
		if (!optionalHBaseTable.isPresent()) {
			throw new IllegalStateException(String.format("There is no table matching '%s'.", predicate));
		}
		return optionalHBaseTable.get();
	}	
}
