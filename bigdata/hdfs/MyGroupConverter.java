package com.chail.apputil.hdfs;

import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;

public class MyGroupConverter extends GroupConverter{
	MessageType schema ;
	GroupConverter rootConverter;
	
	

	public MyGroupConverter(MessageType schema) {
		super();
		this.schema = schema;
		GroupRecordConverter groupRecordConverter = new GroupRecordConverter(schema);
    	 rootConverter = groupRecordConverter.getRootConverter();
	}

	@Override
	public void end() {
		System.out.println("end");
	}

	@Override
	public Converter getConverter(int arg0) {
		GroupConverter asGroupConverter = rootConverter.getConverter(arg0).asGroupConverter();
    	Converter converter = rootConverter.getConverter(arg0);
		return converter ;
	}

	@Override
	public void start() {
		System.out.println("start");
		
	}

}
