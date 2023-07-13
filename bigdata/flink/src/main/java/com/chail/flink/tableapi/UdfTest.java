package com.chail.flink.tableapi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.File;
import java.util.Set;

/**
 * @author : yangc
 * @date :2023/7/11 14:57
 * @description :
 * @modyified By:
 */
public class UdfTest {


    public static void main(String[] args) throws Exception {

        //创建环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()   // 使用流处理模式
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.createTemporarySystemFunction("HashFunction", HashFunction.class);


        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);



        //DDL
        String inSqlDdl = String.format("create table input_t (" +
                "username STRING," +
                "url STRING," +
                "tt BIGINT," +
                "et As TO_TIMESTAMP(FROM_UNIXTIME(tt/1000))," +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH('connector' = 'filesystem','path'='%s','format'='csv')", System.getProperty("user.dir") + File.separator + "bigdata" + File.separator + "flink" + File.separator + "clink.txt");
         tableEnv.executeSql(inSqlDdl);
        //表量
        //tableEnv.toDataStream(tableEnv.sqlQuery("SELECT HashFunction(username) FROM input_t")).print();

        tableEnv.toDataStream(tableEnv.sqlQuery("SELECT username,newWord,nyLength FROM input_t ," +
                "LATERAL TABLE(SplitFunction(username)) AS T(newWord, nyLength)")).print();


        env.execute();
    }


    public static class HashFunction extends ScalarFunction {
        // 接受任意类型输入，返回 INT 型输出
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode();
        }
    }


    public static class SplitFunction extends TableFunction<Tuple2<String,Integer>> {

        public void eval(String str) {
            for (String s : str.split("\\?")) {
                // 使用 collect()方法发送一行数据
                collect(Tuple2.of(s, s.length()));
            }
        }
    }

    // 累加器类型定义
    public static class WeightedAvgAccumulator {
        public long sum = 0;    // 加权和
        public int count = 0;    // 数据个数
    }



    public static class accFunction extends AggregateFunction<Long,WeightedAvgAccumulator> {

        // 累加计算方法，每来一行数据都会调用
        public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer
                iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }


        @Override
        public Long getValue(WeightedAvgAccumulator acc) {
            if (acc.count == 0) {
                return null;    // 防止除数为 0
            } else {
                return acc.sum / acc.count;    // 计算平均值并返回
            }
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        @Override
        public Set<FunctionRequirement> getRequirements() {
            return super.getRequirements();
        }

        @Override
        public boolean isDeterministic() {
            return super.isDeterministic();
        }
    }



    // 聚合累加器的类型定义，包含最大的第一和第二两个数据
    public static class Top2Accumulator {
        public Integer first;
        public Integer second;
    }







}
