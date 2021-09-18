package com.chail.apputil.kafka;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaPro {

	public static void testSendMessageSingleton() throws InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(3);
		for (int i = 1; i <= 10; i++) {
			executor.execute(() -> {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				KafkaProducerSingleton kafkaProducerSingleton = KafkaProducerSingleton.getInstance();
				kafkaProducerSingleton.init("dm2", 3);
				System.out
						.println("当前线程:" + Thread.currentThread().getName() + ",获取的kafka实例:" + kafkaProducerSingleton);
				while(true) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					kafkaProducerSingleton.sendKafkaMessage("{\"col1\":\"abc\",\"name\":\"aaaaaaa\"}");
				}
			

			});
		}

	}

	public static void main(String[] args) throws InterruptedException {
		KafkaPro.testSendMessageSingleton();
	}

}