package org.chail;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class MysqlInc {
    private static DebeziumEngine<ChangeEvent<String, String>> engine;

    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();
        props.setProperty("name", "dbz-engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");

        //offset config begin - 使用文件来存储已处理的binlog偏移量
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "./mysql_offsets.dat");
        props.setProperty("offset.flush.interval.ms", "60000");
        //offset config end

        props.setProperty("database.server.name", "my-app-connector");
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename", "./mysql_dbhistory.txt");

        props.setProperty("database.server.id", "23"); //需要与MySQL的server-id不同
        props.setProperty("database.hostname", "192.168.51.196");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "dmcp");
        props.setProperty("database.password", "Dmcp321!");
        props.setProperty("database.include.list", "mc_center");//要捕获的数据库名
        props.setProperty("table.include.list", "mc_center.chail_test");//要捕获的数据表

        props.setProperty("snapshot.mode", "initial");//全量+增量


        KafkaProducerSingleton kafka = KafkaProducerSingleton.getInstance();
        kafka.init("chail_test",2);

        // 使用上述配置创建Debezium引擎，输出样式为Json字符串格式
        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    System.out.println(record);
                    String value = record.value();
                    // 输出到控制台

                    kafka.sendKafkaMessage(value);

                })
                .using((success, message, error) -> {
                    if (error != null) {
                        error.printStackTrace();
                        // 报错回调
                        System.out.println("------------error, message:" + message + "exception:" + error);
                    }
                    closeEngine(engine);
                })
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        addShutdownHook(engine);
        awaitTermination(executor);

        System.out.println("------------main finished.");
    }

    private static void closeEngine(DebeziumEngine<ChangeEvent<String, String>> engine) {
        try {
            engine.close();
        } catch (IOException ignored) {
        }
    }

    private static void addShutdownHook(DebeziumEngine<ChangeEvent<String, String>> engine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeEngine(engine)));
    }

    private static void awaitTermination(ExecutorService executor) {
        if (executor != null) {
            try {
                executor.shutdown();
                while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
