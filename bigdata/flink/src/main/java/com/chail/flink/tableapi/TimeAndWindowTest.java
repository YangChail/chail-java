package com.chail.flink.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * @author : yangc
 * @date :2023/7/10 15:33
 * @description :
 * @modyified By:
 */
public class TimeAndWindowTest {


    public static void main(String[] args) throws Exception {
        //创建环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()   // 使用流处理模式
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
        //DDL
        String inSqlDdl = String.format("create table input_t (" +
                "username STRING," +
                "url STRING," +
                "tt BIGINT," +
                "et As TO_TIMESTAMP(FROM_UNIXTIME(tt/1000))," +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH('connector' = 'filesystem','path'='%s','format'='csv')", System.getProperty("user.dir")+ File.separator+"bigdata"+File.separator+"flink"+File.separator+"clink.txt");
        TableResult tableResult = tableEnv.executeSql(inSqlDdl);
        //分组聚合
        Table table = tableEnv.sqlQuery("select count(username),username from input_t group by username ");
        //table.printSchema();
        //tableEnv.toChangelogStream(table).print("ugg");


        //滚动窗口聚合

        tableEnv.toDataStream(tableEnv.sqlQuery("select count(username),username,window_end AS endT " +
                "FROM TABLE( TUMBLE(TABLE input_t ,DESCRIPTOR(et),INTERVAL '10' SECOND))" +
                "GROUP BY username,window_end,window_start"))

                //.print("==TUMBLE=WINDOW==")
        ;



        //滑动窗口聚合
        tableEnv.toDataStream(tableEnv.sqlQuery("select count(username),username,window_end AS endT " +
                "FROM TABLE(" +
                " HOP(TABLE input_t ,DESCRIPTOR(et),INTERVAL '5' SECOND,INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY username,window_end,window_start"))
               // .print("==HOP=WINDOW==")

        ;

        //统计窗口聚合
        tableEnv.toDataStream(tableEnv.sqlQuery("select count(username),username,window_end AS endT " +
                        "FROM TABLE(" +
                        " CUMULATE(TABLE input_t ,DESCRIPTOR(et),INTERVAL '5' SECOND,INTERVAL '10' SECOND)" +
                        ")" +
                        "GROUP BY username,window_end,window_start"))
                //.print("==CUMULATE=WINDOW==")
        ;


        //开窗聚合
        tableEnv.toDataStream(tableEnv.sqlQuery("SELECT username," +
                        "  COUNT(url) OVER w AS cnt, " +
                        "  MAX(CHAR_LENGTH(url)) OVER w AS max_url " +
                        "  FROM input_t " +
                        "  WINDOW w AS ( " +
                        "  PARTITION BY username " +
                        "  ORDER BY et " +
                        "  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) "))
                .print("==OVER=WINDOW==");

        env.execute();
    }
}
