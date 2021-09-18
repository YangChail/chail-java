package com.mchz.dm.model.source.bigdata.hbase;

import java.util.ArrayList;
import java.util.List;

public class HBaseInfo implements Comparable<HBaseInfo> {

	private String namespace = "default";
	private String tableName;
	public List<String> colfamliyList = new ArrayList<>();
	List<Column> columnList = new ArrayList<>();
	private String targetNamespace;

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

	public List<String> getColfamliyList() {
		return colfamliyList;
	}

	public void setColfamliyList(List<String> colfamliyList) {
		this.colfamliyList = colfamliyList;
	}

	public List<Column> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<Column> columnList) {
		this.columnList = columnList;
	}

	public static class Column implements Comparable<Column> {
		public String name;
		public String type;

		public Column() {
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Column) {
				return ((Column) obj).getName().equalsIgnoreCase(this.name) ? true : false;
			}
			return super.equals(obj);
		}
		
		@Override
		public int hashCode() {
			return super.hashCode();
		}

		@Override
		public int compareTo(Column o) {
			return this.name.compareTo(o.name);
		}
	}

	public String getTargetNamespace() {
		return targetNamespace;
	}

	public void setTargetNamespace(String targetNamespace) {
		this.targetNamespace = targetNamespace;
	}

	@Override
	public int compareTo(HBaseInfo o) {
		return (this.namespace + this.getTableName()).compareTo((o.namespace + o.getTableName()));
	}

}
