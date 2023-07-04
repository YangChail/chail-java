package com.chail.flink.wortermark;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.time.Duration;

/**
 * @author : yangc
 * @date :2023/7/1 10:38
 * @description :
 * @modyified By:
 */
public class WorterMark {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> stream = env.addSource(new ClinkSource());

        stream.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                           @Override
                                           public long extractTimestamp(Event element, long recordTimestamp) {
                                               return element.getTime();
                                           }
                                       }
                ));

        SingleOutputStreamOperator<String> map = stream.map(new MapFunction<Event, String>() {

            @Override
            public String map(Event value) throws Exception {
                return value.getUser();
            }
        });
        map.print();
        env.execute();

    }
}
