package com.chail.flink.tableapi;

import com.chail.flink.api.source.ClinkSource;
import com.chail.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.time.Duration;

/**
 * @author : yangc
 * @date :2023/7/4 14:41
 * @description :
 * @modyified By:
 */
public class CommonTableTest {

    public static void main(String[] args) {
        //创建环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()   // 使用流处理模式
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        //simple(tableEnv);
        count(tableEnv);

    }



    private static void transStream(TableEnvironment tableEnv){
        //创建输入表
        String inSqlDdl = String.format("create table input_t (username STRING,url STRING,tt BIGINT) WITH('connector' = 'filesystem','path'='%s','format'='csv')", System.getProperty("user.dir")+ File.separator+"bigdata"+File.separator+"flink"+File.separator+"clink.txt");
        tableEnv.executeSql(inSqlDdl);
        //创建输出表
        String outSqlDdl = String.format("create table output_t (username STRING,tt BIGINT) WITH('connector'='print')");
        tableEnv.executeSql(outSqlDdl);
        //转换临时表
        Table sqlQuery = tableEnv.sqlQuery("select username,count(username)as tt from input_t group by username");

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
