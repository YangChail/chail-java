package com.chail.flink.tableapi;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;
import java.time.Duration;

/**
 * @author : yangc
 * @date :2023/7/4 14:41
 * @description :
 * @modyified By:
 */
public class CommonTableTest {

    public static void main(String[] args) throws Exception {
        //创建环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()   // 使用流处理模式
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(),settings);
        //simple(tableEnv);
        //count(tableEnv);
        transStream(tableEnv);

    }


    /**
     * 6> +I[Mary, 1]
     * 10> +I[Bob, 1]
     * 12> +I[Alice, 1]
     * 6> -U[Mary, 1]
     * 10> -U[Bob, 1]
     * 6> +U[Mary, 2]
     * 10> +U[Bob, 2]
     *
     * @param tableEnv
     * @throws Exception
     */
    private static void transStream(StreamTableEnvironment tableEnv) throws Exception {
        //创建输入表
        String inSqlDdl = String.format("create table input_t (username STRING,url STRING,tt BIGINT) WITH('connector' = 'filesystem','path'='%s','format'='csv')", System.getProperty("user.dir")+ File.separator+"bigdata"+File.separator+"flink"+File.separator+"clink.txt");
        tableEnv.executeSql(inSqlDdl);
        //创建输出表
        String outSqlDdl = String.format("create table output_t (username STRING,tt BIGINT) WITH('connector'='print')");
        tableEnv.executeSql(outSqlDdl);
        //转换临时表
        Table sqlQuery = tableEnv.sqlQuery("select username,count(username)as tt from input_t group by username");
        DataStream<Row> changelogStream = tableEnv.toChangelogStream(sqlQuery);
        changelogStream.print();
        changelogStream.executeAndCollect();
    }



    private static void count(TableEnvironment tableEnv){
        //创建输入表
        String inSqlDdl = String.format("create table input_t (username STRING,url STRING,tt BIGINT) WITH('connector' = 'filesystem','path'='%s','format'='csv')", System.getProperty("user.dir")+ File.separator+"bigdata"+File.separator+"flink"+File.separator+"clink.txt");
        tableEnv.executeSql(inSqlDdl);
        //创建输出表
        String outSqlDdl = String.format("create table output_t (username STRING,tt BIGINT) WITH('connector'='print')");
        tableEnv.executeSql(outSqlDdl);
        //转换临时表
        Table sqlQuery = tableEnv.sqlQuery("select username,count(username)as tt from input_t group by username");
        //输出
        sqlQuery.executeInsert("output_t");

    }

    private static void simple(TableEnvironment tableEnv){
        //创建输入表
        String inSqlDdl = String.format("create table input_t (username STRING,url STRING,tt BIGINT) WITH('connector' = 'filesystem','path'='%s','format'='csv')", System.getProperty("user.dir")+ File.separator+"bigdata"+File.separator+"flink"+File.separator+"clink.txt");
        tableEnv.executeSql(inSqlDdl);
        //创建输出表
        String outSqlDdl = String.format("create table output_t (username STRING,url STRING,tt BIGINT) WITH('connector'='print')");
        tableEnv.executeSql(outSqlDdl);
        //转换临时表
        Table sqlQuery = tableEnv.sqlQuery("select * from input_t");
        //输出
        sqlQuery.executeInsert("output_t");
    }

}
