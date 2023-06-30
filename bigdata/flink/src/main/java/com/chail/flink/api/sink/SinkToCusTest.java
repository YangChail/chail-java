package com.chail.flink.api.sink;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;

/**
 * @author : yangc
 * @date :2023/6/30 18:32
 * @description :
 * @modyified By:
 */
public class SinkToCusTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClinkSource());


        stream.sinkTo(new Sink<Event>() {
            @Override
            public SinkWriter<Event> createWriter(InitContext context) throws IOException {
                    return  new SinkWriter<Event>() {
                        @Override
                        public void write(Event element, Context context) throws IOException, InterruptedException {
                            System.out.println(element);
                        }

                        @Override
                        public void flush(boolean endOfInput) throws IOException, InterruptedException {
                            System.out.println("flush");
                        }

                        @Override
                        public void close() throws Exception {
                            System.out.println("close");
                        }
                    };
            }
        });

       /* stream.addSink(new RichSinkFunction<Event>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open");
            }

            @Override
            public void close() throws Exception {
                System.out.println("close");
            }

            @Override
            public void invoke(Event value, Context context) throws Exception {
                System.out.println(context.currentProcessingTime()+"->"+value.toString());
            }

            @Override
            public void finish() throws Exception {
                System.out.println("finish");
            }
        });*/

        env.execute();


    }
}
