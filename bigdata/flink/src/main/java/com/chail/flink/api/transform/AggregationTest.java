package com.chail.flink.api.transform;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : yangc
 * @date :2023/6/30 16:18
 * @description :
 * @modyified By:
 */
public class AggregationTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());

        SingleOutputStreamOperator<Event> ageMax = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {

                return value.getUser();
            }
        }).max("age");

        stream.keyBy(Event::getUser).maxBy("age").print();


        ageMax.print();
        env.execute();

    }

}
