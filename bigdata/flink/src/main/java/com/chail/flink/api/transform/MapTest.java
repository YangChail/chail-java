package com.chail.flink.api.transform;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : yangc
 * @date :2023/6/30 15:22
 * @description :
 * @modyified By:
 */
public class MapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());
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
