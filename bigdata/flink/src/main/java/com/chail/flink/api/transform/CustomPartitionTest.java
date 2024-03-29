package com.chail.flink.api.transform;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : yangc
 * @date :2023/6/30 16:18
 * @description : 自定义分区
 * @modyified By:
 */
public class CustomPartitionTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());
        stream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                if(key.equals("Bob")){
                    return 0;
                }else  if(key.equals("Mary")){
                    return 1;
                }else{
                    return numPartitions-1;
                }
            }
        }, new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return  value.getUser();
            }
        }).print().setParallelism(4);
        env.execute();

    }

}
