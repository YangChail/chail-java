package com.chail.apputil.hdfs;

import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class MyRecordMaterializer extends RecordMaterializer<ParquetData> {
  
	MessageType schema ;
	
	
	


    public MyRecordMaterializer(MessageType schema) {
		super();
		this.schema = schema;
	}

	@Override
    public ParquetData getCurrentRecord() {
		return new ParquetData();
    }

    @Override
    public GroupConverter getRootConverter() {
    	
		return new MyGroupConverter(schema);
    }
  }