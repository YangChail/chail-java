package com.chail.flink.api.transform;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : yangc
 * @date :2023/6/30 16:31
 * @description :
 * @modyified By:
 */
public class ReduceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = stream.map(new MapFunction<Event, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Event value) throws Exception {
                        return Tuple2.of(value.getUser(), 1);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        reduce.print("new->");


         reduce.keyBy(data -> "max").reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {

                return value1.f1 > value2.f1 ? value1 : value2;

            }
        }).print("max->");

        env.execute();



    }

}
