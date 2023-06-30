package com.chail.flink.demo.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.File;

/**
 * @author : yangc
 * @date :2023/6/29 14:52
 * @description :
 * @modyified By:
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        String getenv = System.getProperty("user.dir");
        getenv=getenv+ File.separator+"bigdata"+File.separator+"flink"+File.separator+"word.txt";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> linDatasource = env.readTextFile(getenv);
        AggregateOperator<Tuple2<String, Long>> sum = linDatasource.flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {
            String[] s = line.split(" ");
            for (String s1 : s) {
                collector.collect(Tuple2.of(s1, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .groupBy(0)
                .sum(1);
        sum.print();
    }
}
