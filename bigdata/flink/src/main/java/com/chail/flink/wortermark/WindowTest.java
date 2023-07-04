package com.chail.flink.wortermark;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author : yangc
 * @date :2023/7/1 11:13
 * @description :
 * @modyified By:
 */
public class WindowTest {

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

        //new AggregateFunction<Event, Tuple2<Long, Integer>, Tuple2<String, Integer>>()
        stream.keyBy(data->data.getUser()) .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                        .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, Tuple2<String, Integer>>(){
                            @Override
                            public Tuple2<Long, Integer> createAccumulator() {
                                return null;
                            }

                            @Override
                            public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                                return null;
                            }

                            @Override
                            public Tuple2<String, Integer> getResult(Tuple2<Long, Integer> accumulator) {
                                return null;
                            }

                            @Override
                            public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                                return null;
                            }
                        })
                                .print();


        stream.map(new MapFunction<Event, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Event value) throws Exception {
                        return Tuple2.of(value.getUser(), 1);
                    }
                }).keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();

        env.execute();

    }

}
