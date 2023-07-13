package com.chail.kafka.kafka2kafka;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.chail.kafka.kafka2kafka.parse.DsyncParse;
import com.chail.kafka.kafka2kafka.parse.Record;
import com.chail.kafka.kafka2kafka.parse.ValueColumn;
import kafka.server.KafkaApis;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static com.chail.kafka.kafka2kafka.parse.DSyncPacket.Packet.parseFrom;

/**
 * @author : yangc
 * @date :2023/7/12 9:49
 * @description :
 * @modyified By:
 */
@Slf4j
public class Kafka2KafkaDsync {


    //b3JhY2xlXzEwMDA1X1pIWUxBRE1JTkJTSElTX01TX0dITVg_     "YSCQ_ZHYLADMINBSHIS"."MS_GHMX"


    private static final String SOURCE_BOOTSTRAP_SERVERS = "10.85.2.235:9092";
    private static final String DESTINATION_BOOTSTRAP_SERVERS = "192.168.239.1:9092";

    private static final Map<String, String> topicMap = new HashMap<>();

    private static final long epochMilli = LocalDateTime.of(2023, 7, 10, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();



    static {
        topicMap.put("b3JhY2xlXzEwMDA1X1pIWUxBRE1JTkJTSElTX01TX0dITVg_", "ZHYLADMINBSHIS.MS_GHMX");
        topicMap.put("b3JhY2xlXzEwMDA1X1pIWUxBRE1JTkJTSElTX01TX0JSREE_", "ZHYLADMINBSHIS.MS_BRDA");
        topicMap.put("b3JhY2xlXzEwMDA1X1pIWUxBRE1JTkJTSElTX0dZX1lHRE0_", "ZHYLADMINBSHIS.GY_YGDM");
        topicMap.put("b3JhY2xlXzEwMDA1X1pIWUxBRE1JTkJTSElTX01TX0dIS1M_", "ZHYLADMINBSHIS.MS_GHKS");
        topicMap.put("b3JhY2xlXzEwMDA1X1pIWUxBRE1JTkJTSElTX1lTX01aX0paTFM_", "ZHYLADMINBSHIS.YS_MZ_JZLS");
    }


    public static void main(String[] args) {
        for (String st : topicMap.keySet()) {
            Thread thread = new Thread(new Sync(st,topicMap.get(st)));
            thread.setName(topicMap.get(st));
            thread.start();
        }
    }


    @Slf4j
    public static class Sync implements Runnable {

        private String sourceTopic;
        private String targetTopic;


        public Sync(String sourceTopic, String targetTopic) {
            this.sourceTopic = sourceTopic;
            this.targetTopic = targetTopic;
        }

        @Override
        public void run() {

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP_SERVERS);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "data_transfer_group3"+targetTopic);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DESTINATION_BOOTSTRAP_SERVERS);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(sourceTopic));
            Map<TopicPartition, Long> timestampsToSearch=new HashMap<>();
            TopicPartition topicPartition=new TopicPartition(sourceTopic,0);
            timestampsToSearch.put(topicPartition,epochMilli);
            Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(timestampsToSearch, Duration.ofSeconds(10));
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
            // 指定位置开始消费
            Set<TopicPartition> assignment= new HashSet<>();
            while (assignment.size() == 0) {
                consumer.poll(Duration.ofSeconds(1));
                // 获取消费者分区分配信息（有了分区分配信息才能开始消费）
                assignment = consumer.assignment();
            }

            // 遍历所有分区，并指定 offset 从 100 的位置开始消费
            for (TopicPartition t : assignment) {
                consumer.seek(topicPartition,offsetAndTimestamp.offset()); // 指定offset
            }
            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            AdminClient adminClient = AdminClient.create(producerProps);
            adminClient.deleteTopics(Collections.singletonList(targetTopic));


            boolean first=true;

            int i = 0;
            try {
                while (true) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(100);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        String key = record.key();
                        Object value = record.value();
                        List<Record> records1 = DsyncParse.parseFrom(value);
                        for (Record record1 : records1) {
                            JSONObject json=new JSONObject();
                            Map<String, ValueColumn> columnMap = record1.getColumnMap();
                            columnMap.forEach((k,v)->{
                                String col = k.replace("#", "");
                                Object value1 = v.getData().getValue();
                                if("_mc_execute_time)".equals(col)){
                                    value1=value1 == null ? new Date().getTime() : value1;
                                }
                                json.put(col,value1);
                            });
                            if(first){
                                Long v = json.getLong("_mc_execute_time");


                                log.error("================================="+new Date(v));
                                first=false;
                            }
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic, key, json.toString());
                            producer.send(producerRecord);
                            if (i % 100 == 0) {
                                log.info("{}已经同步{}条", targetTopic, i);
                            }
                            i++;
                        }
                        // 在这里进行数据处理或转换
                    }
                    producer.flush();
                }
            } finally {
                consumer.close();
                producer.close();
            }
        }
    }

}
