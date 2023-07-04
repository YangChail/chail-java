package com.chail.flink.process;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author : yangc
 * @date :2023/7/1 14:39
 * @description :
 * @modyified By:
 */
public class ProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
       DataStream<Event> stream= env.addSource(new ClinkSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTime();
                    }
                }));

        stream.process(new org.apache.flink.streaming.api.functions.ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, org.apache.flink.streaming.api.functions.ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                System.out.println(value.getUser());
                System.out.println(ctx.timestamp());
                System.out.println(getRuntimeContext().getIndexOfThisSubtask());
                System.out.println(getRuntimeContext().getJobId());
            }
        });

        stream.print();

        env.execute();


    }
}
