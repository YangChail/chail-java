package com.chail.flink.tableapi;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author : yangc
 * @date :2023/7/4 14:41
 * @description :
 * @modyified By:
 */
public class SimpleTableTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        DataStream<Event> stream = env.addSource(new ClinkSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTime();
                    }
                }));
        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //流转表
        Table table = tableEnv.fromDataStream(stream);

        //sql 查询
        Table res = tableEnv.sqlQuery("select user,url from " + table);
        //代码组装
        //Table res2 = table.select($("url"), $("user")).where($("user").isEqual("Bob"));
        tableEnv.toDataStream(res).print();

        //tableEnv.toDataStream(res2).print("res2");
    }
}
