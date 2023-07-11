package com.chail.flink.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * @author : yangc
 * @date :2023/7/10 16:57
 * @description :
 * @modyified By:
 */
public class TopNTest {


    public static void main(String[] args) throws Exception {

        //创建环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()   // 使用流处理模式
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //DDL
        String inSqlDdl = String.format("create table input_t (" +
                "username STRING," +
                "url STRING," +
                "tt BIGINT," +
                "et As TO_TIMESTAMP(FROM_UNIXTIME(tt/1000))," +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH('connector' = 'filesystem','path'='%s','format'='csv')", System.getProperty("user.dir") + File.separator + "bigdata" + File.separator + "flink" + File.separator + "clink.txt");
        TableResult tableResult = tableEnv.executeSql(inSqlDdl);

        Table table = tableEnv.sqlQuery("select username,cnt,row_num from" +
                "( select * ,ROW_NUMBER() OVER( order by cnt DESC) AS row_num from (select username,count(url) as cnt from input_t group by username)  " +
                ") where row_num <=2");
        //tableEnv.toChangelogStream(table).print();

        // 定义子查询，进行窗口聚合，得到包含窗口信息、用户以及访问次数的结果表
        String subQuery =
                "SELECT window_start, window_end, username, COUNT(url) as cnt " +
                        "FROM TABLE (TUMBLE( TABLE input_t, DESCRIPTOR(et), INTERVAL '10' SECOND )) GROUP BY window_start, window_end, username ";

        Table table2 = tableEnv.sqlQuery("select username,cnt,row_num from" +
                "( " +
                "select * ,ROW_NUMBER() OVER( partition by window_start,window_end  order by cnt DESC) AS row_num from (" + subQuery + ")  " +
                ") where row_num <=2");
        tableEnv.toDataStream(table2).print();

        env.execute();


    }
}
