package com.chail.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

public class FlinkCDC_Mysql {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(100); //100ms进行一次数据的备份，默认10ms
        //如果不设置该属性就是DELETE_ON_CANCELLATION
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //env.setStateBackend(new FsStateBackend("file:///Users/dhg/Documents/exactlyOnce")); //checkpoint状态保存位置
        env.getCheckpointConfig().setCheckpointStorage("file://D://checkpoints");

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(2))); //重启2次，间隔2s，不配置的话，就是无限重启
        env.setParallelism(1);

        Properties prop = new Properties();
        prop.setProperty("autoReconnect","true");

        //创建 Flink-MySQL-CDC 的 Source
        //initial (default): 在第一次启动时对被监视的数据库表执行初始快照，并继续读取最新的binlog (开启断点续传后从上次消费offset继续消费)
        //latest-offset: 永远不要在第一次启动时对被监视的数据库表执行快照，只从binlog的末尾读取，这意味着只有自连接器启动以来的更改
        //timestamp: 永远不要在第一次启动时对监视的数据库表执行快照，直接从指定的时间戳读取binlog。使用者将从头遍历binlog，并忽略时间戳小于指定时间戳的更改事件
        //specific-offset: 不允许在第一次启动时对监视的数据库表进行快照，直接从指定的偏移量读取binlog。
        MySqlSource<String> build = MySqlSource.<String>builder()
                .serverTimeZone("UTC")
                .hostname("192.168.23.191")
                .port(3307)
                .username("root")
                .password("_Abcd123456")
                .databaseList("bns")
                //tableList为可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .tableList("bns.bns_order","bns.file_import","bns.poll_info")
                .startupOptions(StartupOptions.latest())
                //自定义反序列化器
                .deserializer(new FlinkCdcDataDeserializationSchema())
                //jdbc连接参数配置
                .jdbcProperties(prop)
                .build();


        //使用 CDC Source 从 MySQL 读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(build, WatermarkStrategy.noWatermarks(), "MysqlSource");

        //打印数据
        mysqlDS.printToErr("------>").setParallelism(1);

//        mysqlDS.addSink(new MysqlSink());

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "1.15.66.155:9092");
        properties.setProperty("group.id", "kafka");
        properties.setProperty("auto.offset.reset", "latest"); //从当前开始读取数据
        //如果开启事务，需要将客户端的事务超时间小于broker的事务超时时间
        properties.setProperty("transaction.timeout.ms", "10"); //broker默认值是为15分钟
        String topic  = "my-topic";

        mysqlDS.addSink(new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        )).name("mysqlDS kafka write").setParallelism(1);



        //6.执行任务
        env.execute("flink kafka write");

        /**
         * 回放binlog ？
         */


    }
}