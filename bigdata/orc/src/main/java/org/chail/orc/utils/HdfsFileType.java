package org.chail.orc.utils;

import org.apache.commons.lang.StringUtils;

/**
 * 字段类型枚举
 */
public enum HdfsFileType {
	TEXT("TEXT", "org.apache.hadoop.mapred.TextInput","TEXTFILE"),
	PARQUET("PARQUET", "org.apache.hadoop.hive.ql.io.parquet","PARQUET"),
	ORC("ORC", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat","ORC"),
	SEQ("SEQ", "org.apache.hadoop.mapred.SequenceFileInputFormat","SEQ"),
	RC("RC", "org.apache.hadoop.hive.ql.io.RCFileInputFormat","RC"),
	AVRO("AVRO", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat","AVRO"),
	HYPERDRIVE("Hyperdrive", "io.transwarp.hyperdrive.HyperdriveStorageHandler","Hyperdrive"),
	CSV("CSV", "org.apache.hadoop.hive.ql.io.csv.CSVNLineInputFormat","CSV");

	private String value;
	private String hiveClassName;
	//TODO 需要去查 所有的
	private String impalaStoreAsName;

	private HdfsFileType(String value, String hiveClassName, String impalaStoreAsName) {
		this.value = value;
		this.hiveClassName = hiveClassName;
		this.impalaStoreAsName = impalaStoreAsName;
	}

	public String getValue() {
		return value;
	}

	public String getHiveClassName() {
		return hiveClassName;
	}

	public void setHiveClassName(String hiveClassName) {
		this.hiveClassName = hiveClassName;
	}

	public String getImpalaStoreAsName() {
		return impalaStoreAsName;
	}

	public void setImpalaStoreAsName(String impalaStoreAsName) {
		this.impalaStoreAsName = impalaStoreAsName;
	}

	// 通过value获取对应的枚举对象
	public static HdfsFileType getHdfsFileType(String value) {
		for (HdfsFileType type : HdfsFileType.values()) {
			if (StringUtils.equalsIgnoreCase(type.getValue(), value)) {
				return type;
			}
		}
		return null;
	}
}
