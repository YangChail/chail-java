package com.chail.flink.process;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author : yangc
 * @date :2023/7/1 14:46
 * @description :
 * @modyified By:
 */
public class KeyedProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        DataStream<Event> stream = env.addSource(new ClinkSource());
        stream.keyBy(data -> data.getUser()).process(new org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event value, org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                long l = ctx.timerService().currentProcessingTime();
                out.collect("----"+new Timestamp(l ));



                ctx.timerService().registerEventTimeTimer(l+10*1000);

            }


            @Override
            public void onTimer(long timestamp, org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }
        });


        stream.print();

        env.execute();


    }


}
