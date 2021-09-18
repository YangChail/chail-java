package com.chail.apputil.hdfs;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class MyParquetReadSupport extends ReadSupport<ParquetData> {
	MessageType schema;

	public MyParquetReadSupport(MessageType schema) {
		super();
		this.schema = schema;
	}

	@Override
	public ReadContext init(InitContext context) {
		return new ReadContext(schema, new HashMap<>());
	}

	@Override
	public RecordMaterializer<ParquetData> prepareForRead(Configuration arg0, Map<String, String> arg1,
			MessageType arg2, ReadContext arg3) {

		return new MyRecordMaterializer(schema);
	}

}
