package com.chail.flink.api.transform;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : yangc
 * @date :2023/6/30 17:18
 * @description :
 * @modyified By:
 */
public class RichFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());


        stream.map(new RichMapFunction<Event, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println(getRuntimeContext().getJobId());
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public Integer map(Event value) throws Exception {
                return value.getAge();
            }
        }).print();

        env.execute();

    }
}
