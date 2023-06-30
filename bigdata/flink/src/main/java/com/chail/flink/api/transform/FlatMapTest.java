package com.chail.flink.api.transform;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author : yangc
 * @date :2023/6/30 15:22
 * @description :
 * @modyified By:
 */
public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());
        SingleOutputStreamOperator<String> map = stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                if("Alice".equals(value.getUser())){
                    out.collect(value.getUser());
                }else if ("Bob".equals(value.getUser())){
                    out.collect(value.getUser());
                    out.collect(value.getUrl());
                }
            }
        });
        map.print();
        env.execute();

    }
}
