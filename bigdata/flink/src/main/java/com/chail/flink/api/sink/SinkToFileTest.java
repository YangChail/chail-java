package com.chail.flink.api.sink;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author : yangc
 * @date :2023/6/30 16:18
 * @description : 自定义分区
 * @modyified By:
 */
public class SinkToFileTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());
//        stream.partitionCustom(new Partitioner<String>() {
//            @Override
//            public int partition(String key, int numPartitions) {
//                if(key.equals("Bob")){
//                    return 0;
//                }else  if(key.equals("Mary")){
//                    return 1;
//                }else{
//                    return numPartitions-1;
//                }
//            }
//        }, new KeySelector<Event, String>() {
//            @Override
//            public String getKey(Event value) throws Exception {
//                return  value.getUser();
//            }
//        }).print();

        StreamingFileSink<String> streamingFilesink = StreamingFileSink.<String>forRowFormat(new Path("./output")
                        , new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1)).build()
                ).build();

        stream.map(data -> data.toString()).addSink(streamingFilesink);

        env.execute();

    }

}
