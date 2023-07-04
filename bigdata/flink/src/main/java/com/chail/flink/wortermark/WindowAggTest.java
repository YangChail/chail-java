package com.chail.flink.wortermark;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author : yangc
 * @date :2023/7/1 12:14
 * @description :
 * @modyified By:
 */
public class WindowAggTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> stream = env.addSource(new ClinkSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                                           @Override
                                                           public long extractTimestamp(Event element, long recordTimestamp) {
                                                               return element.getTime();
                                                           }
                                                       }
                                ));
        stream.print();

        stream.keyBy(data->true)
                .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>() {
                    @Override
                    public Tuple2<Long, HashSet<String>> createAccumulator() {
                        return Tuple2.of(0L,new HashSet<>());
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
                        accumulator.f1.add(value.getUser());
                        return Tuple2.of(accumulator.f0+1,accumulator.f1) ;
                    }

                    @Override
                    public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
                        return (double) (accumulator.f0 / accumulator.f1.size());
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
                        return null;
                    }
                })
        ;




        env.execute();

    }
}
