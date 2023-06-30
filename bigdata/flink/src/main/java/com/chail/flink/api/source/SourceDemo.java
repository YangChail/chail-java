package com.chail.flink.api.source;

import com.chail.flink.model.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        DataStreamSource<Event> dataStreamSource = env.addSource(new ClinkSource());
        dataStreamSource.print();
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
