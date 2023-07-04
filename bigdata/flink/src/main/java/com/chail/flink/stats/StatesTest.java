package com.chail.flink.stats;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : yangc
 * @date :2023/7/1 14:39
 * @description :
 * @modyified By:
 */
public class StatesTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        DataStream<Event> stream = env.addSource(new ClinkSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTime();
                    }
                }));



        stream.addSink(new BufferingSink(10));


        env.execute();


    }


    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        private int threshold = 10;


        private List<Event> buffer;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.buffer = new ArrayList<>();
        }


        private ListState<Event> ckptState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            buffer.add(value);

            if (buffer.size() >= threshold) {
                System.out.println(buffer.toString());
                buffer.clear();
            }


        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            ckptState.clear();
            for (Event event : buffer) {
                ckptState.add(event);
            }

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 注册
            ListStateDescriptor<Event> des = new ListStateDescriptor<>("bufState", Event.class);
            context.getOperatorStateStore().getListState(des);
            // 如果是从故障中恢复，就将 ListState 中的所有元素添加到局部变量中
            if (context.isRestored()) {
                for (Event element : ckptState.get()) {
                    buffer.add(element);
                }
            }
        }
    }

}
