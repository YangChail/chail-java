package com.chail.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaProducerSingleton {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerSingleton.class);

	private static KafkaProducer<String, String> kafkaProducer;

	private Random random = new Random();

	private String topic;

	private int retry;

	private KafkaProducerSingleton() {

	}

	/**
	 * 静态内部类
	 * 
	 * @author tanjie
	 * 
	 */
	private static class LazyHandler {

		private static final KafkaProducerSingleton instance = new KafkaProducerSingleton();
	}

	/**
	 * 单例模式,kafkaProducer是线程安全的,可以多线程共享一个实例
	 * 
	 * @return
	 */
	public static final KafkaProducerSingleton getInstance() {
		return LazyHandler.instance;
	}

	/**
	 * kafka生产者进行初始化
	 * 
	 * @return KafkaProducer
	 */
	public void init(String topic, int retry) {
		this.topic = topic;
		this.retry = retry;
		if (null == kafkaProducer) {
			Properties properties = new Properties();
			properties.put("zookeeper.connect", "192.168.241.104:2181");// 声明zk
			//properties.put("serializer.class", StringEncoder.class.getName());
			properties.put("bootstrap.servers", "192.168.241.104:9092");// 声明kafka broker
			properties.put("auto.commit.enable", "true");
			properties.put("auto.commit.interval.ms", "60000");
			//properties.put("group.id", "dm2");
			//properties.put( CommonClientConfigs.CLIENT_ID_CONFIG, "dm2");
			properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			kafkaProducer = new KafkaProducer<String, String>(properties);
		}
	}

	/**
	 * 通过kafkaProducer发送消息
	 * 
	 * @param topic        消息接收主题
	 * @param partitionNum 哪一个分区
	 * @param retry        重试次数
	 * @param message      具体消息值
	 */
	public void sendKafkaMessage(final String message) {
		/**w
		 * 1、如果指定了某个分区,会只讲消息发到这个分区上 2、如果同时指定了某个分区和key,则也会将消息发送到指定分区上,key不起作用
		 * 3、如果没有指定分区和key,那么将会随机发送到topic的分区中 4、如果指定了key,那么将会以hash<key>的方式发送到分区中
		 */
		List<String> keys=Arrays.asList(new String[]{"key-one","key-two","key-three","key-four"});
		 int nextInt = random.nextInt(4);
		 String key = keys.get(nextInt);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,nextInt, keys.get(0),message);
		//ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,message);
		
		// send方法是异步的,添加消息到缓存区等待发送,并立即返回，这使生产者通过批量发送消息来提高效率
		// kafka生产者是线程安全的,可以单实例发送消息
		kafkaProducer.send(record, new Callback() {
			public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
				if (null != exception) {
					LOGGER.error("kafka发送消息失败:" + exception.getMessage(), exception);
					retryKakfaMessage(message);
				}else {
					LOGGER.info("Send success topic:{}\t key:{}\t patition:{}\t message:{}",topic,key,nextInt,message);
				}
			}
		});
	}

	/**
	 * 当kafka消息发送失败后,重试
	 * 
	 * @param retryMessage
	 */
	private void retryKakfaMessage(final String retryMessage) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, random.nextInt(3), "",
				retryMessage);
		for (int i = 1; i <= retry; i++) {
			try {
				kafkaProducer.send(record);
				return;
			} catch (Exception e) {
				LOGGER.error("kafka发送消息失败:" + e.getMessage(), e);
				retryKakfaMessage(retryMessage);
			}
		}
	}

	/**
	 * kafka实例销毁
	 */
	public void close() {
		if (null != kafkaProducer) {
			kafkaProducer.close();
		}
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getRetry() {
		return retry;
	}

	public void setRetry(int retry) {
		this.retry = retry;
	}

}