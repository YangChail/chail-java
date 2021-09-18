package com.mchz.bigdata.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HBaseTable {

	private HBaseUtil hbaseUtil;
	private String namespace;
	private String tableName;
	private List<String> columnFamilies;
	private List<HBaseColumn> hbaseColumnList;
	

	public HBaseTable(String namespace, String tableName, HBaseUtil hbaseUtil) {
		this.namespace = namespace;
		this.tableName = tableName;
		this.hbaseUtil = hbaseUtil;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumnFamilies() {
		return columnFamilies;
	}

	public void setColumnFamilies(List<String> columnFamilies) {
		this.columnFamilies = columnFamilies;
	}

	public List<HBaseColumn> getHbaseColumnList() {
		return hbaseColumnList;
	}

	public void setHbaseColumnList(List<HBaseColumn> hbaseColumnList) {
		this.hbaseColumnList = hbaseColumnList;
	}


	public String getNameSpaceAndTableName() {
		return namespace+":"+ tableName;
	}
	
	public String getNameSpaceAndTableNameSQL() {
		return namespace+".\""+ tableName + "\"";
	}

	public String getNameSQL() {
		return namespace+".\""+ tableName + "\"";
	}	
	
	public Iterator<Result> iterator(int maxSize) {
		String nameSpaceAndTableName = this.getNameSpaceAndTableName();
		ResultScanner scanner = this.hbaseUtil.getScanner(nameSpaceAndTableName, maxSize);
		return scanner.iterator();

	}

	public Stream<Result> rows(int maxSize) {
		return rowsMatching(row -> true, maxSize);
	}

	public Stream<Result> rowsMatching(Predicate<Result> predicate, int maxSize) {
		Spliterator<Result> s = Spliterators.spliteratorUnknownSize(iterator(maxSize), 0);
		Stream<Result> stream = StreamSupport.stream(s, false).filter(predicate);
		return maxSize>0?stream.limit(maxSize):stream;
	}
	
    @Override
    public String toString() {
        return "HbaseTable{" +
            "namespace='" + namespace + '\'' +
            ", tableName='" + tableName + '\'' +
            ", columnFamilies=" + columnFamilies +
            ", hbaseColumnList=" + hbaseColumnList +
            '}';
    }
}
