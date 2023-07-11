package com.chail.flink.tableapi;

import com.chail.flink.model.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author : yangc
 * @date :2023/7/10 14:46
 * @description :
 * @modyified By:
 */
public class DynamicTableTest {

    public static void main(String[] args) throws Exception {
        // 获取流环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L,1),
                        new Event("Bob", "./cart", 1000L,1),
                        new Event("Alice", "./prod?id=1", 5 * 1000L,1),
                        new Event("Cary", "./home", 60 * 1000L,1),
                        new Event("Bob", "./prod?id=3", 90 * 1000L,1),
                        new Event("Alice", "./prod?id=7", 105 * 1000L,1));

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表
        tableEnv.createTemporaryView("EventTable", eventStream, $("user"), $("url"),
                $("time").as("ts"));

        // 统计每个用户的点击次数
        Table urlCountTable = tableEnv.sqlQuery("SELECT user, COUNT(url) as cnt FROM EventTable GROUP BY user");
        Table aliceVisitTable = tableEnv.sqlQuery("SELECT url, user FROM EventTable WHERE user = 'Cary'");
        // 将表转换成数据流，在控制台打印输出
        //tableEnv.toChangelogStream(urlCountTable).print("count");
        tableEnv.toDataStream(aliceVisitTable).print("count2");
        // 执行程序
        env.execute();
    }
}
