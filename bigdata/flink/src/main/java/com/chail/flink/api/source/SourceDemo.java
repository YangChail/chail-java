package com.chail.flink.api.source;

import com.chail.flink.model.Event;
import com.mchz.flink.connector.jdbc.JdbcConnectionOptions;
import com.mchz.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import com.mchz.flink.connector.jdbc.table.JdbcDynamicTableSource;
import com.mchz.mcdatasource.core.DataBaseType;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;

/**
 * @author : yangc
 * @date :2023/6/30 9:52
 * @description :
 * @modyified By:
 */
public class SourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        Connection connection = new SimpleJdbcConnectionProvider(options).getOrEstablishConnection();
//        System.out.println(connection);



        env.execute();



        //kakfa--from source
        /*KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> stringDataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
*/

    }
}
