package com.chail.flink.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class LoginFailDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取登录事件流，并提取时间戳、生成水位线 
        SingleOutputStreamOperator<LoginEvent> loginEventSingleOutputStreamOperator = env
                .fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                                        return loginEvent.timestamp;
                                    }
                                }
                        )
                );
        KeyedStream<LoginEvent, String> stream = loginEventSingleOutputStreamOperator.keyBy(r -> r.userId);
        //定义数据流

        //定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.eventType.equals("fail");
            }
        }).next("second").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.eventType.equals("fail");
            }
        }).next("third").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.eventType.equals("fail");
            }
        });

        //定义匹配模式与流
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        //选择模式
        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent first = map.get("first").get(0);
                LoginEvent second = map.get("second").get(0);
                LoginEvent third = map.get("third").get(0);
                return first.userId + " 连续三次登录失败！登录时间：" +
                        first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
            }
        });
        select.print();
        env.execute();
    }

}