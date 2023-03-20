package com.chail.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class KafkaUtils {

	public static Properties getPro(String group) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "127.0.0.1:1527");
		props.put("group.id", group);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}
	
	public void getPartition(String topic,KafkaConsumer<String,String> consumer) {
		 List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
		 
		 
	
	}
	
	
	private void getSize(KafkaConsumer<String,String> consumer, Collection<TopicPartition> partitionsFor) {
		Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitionsFor);
		endOffsets.forEach((k,v)->{
			System.out.println(k.topic());
			System.out.println(k.partition());
			System.out.println(v.longValue());
			
		});
		
		
	
	}

	public static void main(String[] args) {
		String topic="dm2";
	     KafkaConsumer<String,String> consumer = new KafkaConsumer<>(getPro("utils"));
	     Set<TopicPartition> assignment = consumer.assignment();
	     List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
	     
	     
	  //   getSize(consumer,assignment);
		
	}
	
}
