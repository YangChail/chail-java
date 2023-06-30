package com.chail.flink.demo.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * @author : yangc
 * @date :2023/6/29 15:37
 * @description :
 * @modyified By:
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        //linux: nc -lk 7777
        StreamExecutionEnvironment env =StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<String> lineDataSteamSource =null;
        boolean useFile=false;
        if(useFile){
            String path = System.getProperty("user.dir");
            path=path+ File.separator+"bigdata"+File.separator+"flink"+File.separator+"word.txt";
            lineDataSteamSource = env.readTextFile(path);
        }else{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String host = parameterTool.get("host", "192.168.239.3");
            int port = parameterTool.getInt("port", 7777);
            lineDataSteamSource = env.socketTextStream(host,port);
        }
        //获取流
        SingleOutputStreamOperator<Tuple2<String, Long>> lineOperator = lineDataSteamSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, collector) -> {
            String[] s = line.split(" ");
            for (String s1 : s) {
                collector.collect(Tuple2.of(s1, 1L));
            }

        }).returns(Types.TUPLE(Types.STRING, Types.LONG));


        //分组
        KeyedStream<Tuple2<String, Long>, Object> keyedStream =
                lineOperator.keyBy(stringLongTuple2 -> stringLongTuple2.f0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        //打印
        sum.print();

        //启动执行
        env.execute();

    }



}
