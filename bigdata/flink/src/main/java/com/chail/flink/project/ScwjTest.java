package com.chail.flink.project;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author : yangc
 * @date :2023/7/12 11:46
 * @description :
 * @modyified By:
 */
public class ScwjTest {


    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()   // 使用流处理模式
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        // 获取表环境的配置
        TableConfig tableConfig = tableEnv.getConfig();
        // 配置状态保持时间
        tableConfig.setIdleStateRetention(Duration.ofMinutes(10));


        TableResult tableResult = tableEnv.executeSql(sourceTable1);


        TableResult tableResult1 = tableEnv.executeSql(sourceTable2);
        TableResult tableResult2 = tableEnv.executeSql(sourceTable3);
        TableResult tableResult3 = tableEnv.executeSql(sourceTable4);
        TableResult tableResult4 = tableEnv.executeSql(sourceTable5);
        //tableEnv.toDataStream(tableEnv.sqlQuery("select et from GY_YGDM")).print();


        String querySql="SELECT * " +
                " FROM " +
                " (select * FROM TABLE( TUMBLE(TABLE MS_GHMX  ,DESCRIPTOR(et),INTERVAL '1' MINUTES))) AS T1" +
                " INNER JOIN (select * FROM TABLE( TUMBLE(TABLE MS_BRDA  ,DESCRIPTOR(et),INTERVAL '1' MINUTES))) AS T0 ON " +
                " T1.BRID = T0.BRID " +
//                " INNER JOIN (select * FROM TABLE( TUMBLE(TABLE GY_YGDM  ,DESCRIPTOR(et),INTERVAL '1' MINUTES))) AS  T3 ON " +
//                " T1.CZGH = T3.YGDM " +
                " INNER JOIN  (select * FROM TABLE( TUMBLE(TABLE MS_GHKS  ,DESCRIPTOR(et),INTERVAL '1' MINUTES))) AS  T4 ON " +
                " T1.KSDM = T4.KSDM " +
                " INNER JOIN  (select * FROM TABLE( TUMBLE(TABLE YS_MZ_JZLS  ,DESCRIPTOR(et),INTERVAL '1' MINUTES)))  AS T6 ON " +
                " T1.SBXH = T6.GHXH " +
                " WHERE " +
                " (T4.DMDZ IS NOT NULL " +
                " AND T1.GHSJ>'2022-12-31')";


//        String querySql="SELECT * " +
//                " FROM " +
//                " MS_GHMX AS T1" +
//                " INNER JOIN MS_BRDA AS T0 ON " +
//                " T1.BRID = T0.BRID " +
////                " INNER JOIN GY_YGDM AS  T3 ON " +
////                " T1.CZGH = T3.YGDM " +
//                " INNER JOIN  MS_GHKS AS  T4 ON " +
//                " T1.KSDM = T4.KSDM " +
//                " INNER JOIN  YS_MZ_JZLS  AS T6 ON " +
//                " T1.SBXH = T6.GHXH " +
//                " WHERE " +
//                " (T4.DMDZ IS NOT NULL " +
//                " AND T1.GHSJ>'2022-12-31'" +
//                ")"
//                ;

       tableEnv.toDataStream(tableEnv.sqlQuery(querySql)).print();
        env.execute();
    }

    private static final String sourceTable1=" create table MS_GHMX(XJJE String," +
            "_mc_table String," +
            "CZGH String," +
            "YSDM String," +
            "ZHLB String," +
            "GHLY String," +
            "SMBZ String," +
            "BLJE String," +
            "YSPB String," +
            "_mc_execute_time BIGINT," +
            "ZJFY String," +
            "JZZT String," +
            "JZRQ String," +
            "GHCS String," +
            "YBYDZFBZ String," +
            "YDZFJE String," +
            "GHLB String," +
            "CZPB String," +
            "JGID String," +
            "JZJS String," +
            "FZJLID String," +
            "_mc_schema String," +
            "ZHJE String," +
            "DZSB String," +
            "SFFS String," +
            "ZLJE String," +
            "MZLB String," +
            "JZYS String," +
            "BRXZ String," +
            "ZDJG String," +
            "YBYDZFJE String," +
            "_mc_db_lsn String," +
            "_mc_db_rowid String," +
            "YYBZ String," +
            "KSDM String," +
            "FYJMBZ String," +
            "ZLFS String," +
            "SBXH String," +
            "GHSJ String," +
            "ZPJE String," +
            "QTYS String," +
            "_mc_sequence String," +
            "JZXH String," +
            "HBWC String," +
            "THBZ String," +
            "HZRQ String," +
            "BRID String," +
            "JZHM String," +
            "FYJM String," +
            "_mc_operation String," +
            "GHJE String," +
            " et as TO_TIMESTAMP(FROM_UNIXTIME(_mc_execute_time/1000, 'yyyy-MM-dd HH:mm:ss'))," +
            " WATERMARK FOR et AS et - INTERVAL '5' SECOND " +
            ") WITH (" +
            "    'connector' = 'kafka'," +
            "    'topic' = 'ZHYLADMINBSHIS.MS_GHMX'," +
            "    'properties.bootstrap.servers' = '192.168.239.1:9092'," +
            "    'format' = 'json'," +
            "    'properties.group.id'='1dscccuaa',"+
            "'scan.startup.mode' = 'earliest-offset',"+
            " 'json.fail-on-missing-field' = 'false'," +
            " 'json.ignore-parse-errors' = 'true'" +
            ");";



    private static final String sourceTable2=" create table GY_YGDM(KCFQ String," +
            "JSYQ String," +
            "YGQM String," +
            "_mc_table String," +
            "YGMM String," +
            "ZJPB String," +
            "JSZC String," +
            "KSSQX String," +
            "RYSCBZ String," +
            "XZZW String," +
            "FWBMOLD String," +
            "MZYQ String," +
            "KSYQ String," +
            "CSNY String," +
            "SFQKYS String," +
            "_mc_execute_time BIGINT," +
            "ZJFY String," +
            "JZSJ_SW String," +
            "YGXM String," +
            "GZSJ String," +
            "ZXJS String," +
            "LXDH String," +
            "ZFPB String," +
            "JGID String," +
            "SJHM String," +
            "WBDM String," +
            "YGDM String," +
            "_mc_schema String," +
            "JZSJ_XW String," +
            "SFSC String," +
            "LZSJ String," +
            "BZXX String," +
            "TSKSSQ String," +
            "QTDM String," +
            "SC String," +
            "_mc_db_lsn String," +
            "_mc_db_rowid String," +
            "YGXB String," +
            "KSDM String," +
            "YXDZ String," +
            "_mc_sequence String," +
            "ZYLB String," +
            "YGYID String," +
            "YGZW String," +
            "SJZH String," +
            "KSSQ String," +
            "SFZH String," +
            "QYQX String," +
            "YGZP String," +
            "BTMM String," +
            "BZQK String," +
            "ZXDM String," +
            "YGJB String," +
            "MZHYS String," +
            "YSJJ String," +
            "XL String," +
            "MZDZBL String," +
            "ZXMM String," +
            "JXDM String," +
            "PYDM String," +
            "_mc_operation String," +
            "ZYFW String," +
            "YGBH String," +
            "FWBM String," +
            "YGJS String" +
            ", et as TO_TIMESTAMP(FROM_UNIXTIME(_mc_execute_time/1000, 'yyyy-MM-dd HH:mm:ss'))," +
            " WATERMARK FOR et AS et - INTERVAL '5' SECOND " +
            ") WITH (" +
            "    'connector' = 'kafka'," +
            "    'topic' = 'ZHYLADMINBSHIS.GY_YGDM'," +
            "    'properties.bootstrap.servers' = '192.168.239.1:9092'," +
            "    'format' = 'json'," +
            "    'properties.group.id'='dscccuaa',"+
            "'scan.startup.mode' = 'earliest-offset',"+
            " 'json.fail-on-missing-field' = 'false'," +
            " 'json.ignore-parse-errors' = 'true'" +
            ");";



    private static final String sourceTable3=" create table YS_MZ_JZLS(_mc_schema String," +
            "JSSJ String," +
            "JZXH String," +
            "ZYZD String," +
            "_mc_table String," +
            "_mc_execute_time BIGINT," +
            "JZZT String," +
            "BRBH String," +
            "YSDM String," +
            "SFSJ String," +
            "GHXH String," +
            "_mc_db_lsn String," +
            "_mc_db_rowid String," +
            "KSDM String," +
            "FZRQ String," +
            "GHFZ String," +
            "_mc_operation String," +
            "YYXH String," +
            "JGID String," +
            "KSSJ String," +
            "_mc_sequence String" +
            ", et as TO_TIMESTAMP(FROM_UNIXTIME(_mc_execute_time/1000, 'yyyy-MM-dd HH:mm:ss'))," +
            " WATERMARK FOR et AS et - INTERVAL '5' SECOND " +
            ") WITH (" +
            "    'connector' = 'kafka'," +
            "    'topic' = 'ZHYLADMINBSHIS.YS_MZ_JZLS'," +
            "    'properties.bootstrap.servers' = '192.168.239.1:9092'," +
            "    'format' = 'json'," +
            "    'properties.group.id'='2dscccuaa',"+
            "'scan.startup.mode' = 'earliest-offset',"+
            " 'json.fail-on-missing-field' = 'false'," +
            " 'json.ignore-parse-errors' = 'true'" +
            ");";



    private static final String sourceTable4=" create table MS_GHKS(SXH String," +
            "GHF String," +
            "_mc_table String," +
            "YMFYYF String," +
            "KSJJ String," +
            "BLYJC String," +
            "SYRCBL String," +
            "XYFYYF String," +
            "TJF String," +
            "SFYY String," +
            "DDDM String," +
            "ZXPB String," +
            "DDXX String," +
            "_mc_execute_time BIGINT," +
            "GHXE String," +
            "YPHDBL String," +
            "GHLB String," +
            "JGID String," +
            "WBDM String," +
            "ZCYFYYF String," +
            "KSYPBL String," +
            "_mc_schema String," +
            "GHRQ String," +
            "YPJCZ String," +
            "KSSJCZ String," +
            "MZLB String," +
            "YYRS String," +
            "QTDM String," +
            "CYFYYF String," +
            "KLFYYF String," +
            "JJRGHF String," +
            "ZZQFBM String," +
            "TJPB String," +
            "_mc_db_lsn String," +
            "_mc_db_rowid String," +
            "KSFZR String," +
            "KSDM String," +
            "ZLF String," +
            "HDCYJTFY String," +
            "_mc_sequence String," +
            "KSMC String," +
            "SYZSJCZ String," +
            "BZKSMC String," +
            "JZXH String," +
            "YGRS String," +
            "MZKS String," +
            "MZHYS String," +
            "JXDM String," +
            "PYDM String," +
            "ZJMZ String," +
            "_mc_operation String," +
            "SCCGBZ String," +
            "DMDZ String" +
            ", et as TO_TIMESTAMP(FROM_UNIXTIME(_mc_execute_time/1000, 'yyyy-MM-dd HH:mm:ss'))," +
            " WATERMARK FOR et AS et - INTERVAL '5' SECOND " +
            ") WITH (" +
            "    'connector' = 'kafka'," +
            "    'topic' = 'ZHYLADMINBSHIS.MS_GHKS'," +
            "    'properties.bootstrap.servers' = '192.168.239.1:9092'," +
            "    'format' = 'json'," +
            "    'properties.group.id'='3dscccuaa',"+
            "'scan.startup.mode' = 'earliest-offset',"+
            " 'json.fail-on-missing-field' = 'false'," +
            " 'json.ignore-parse-errors' = 'true'" +
            ");";



    private static final String sourceTable5=" create table MS_BRDA(LXDZ String," +
            "CSD_SQS String," +
            "_mc_table String," +
            "KNYE String," +
            "GJDM String," +
            "XZZ_QTDZ String," +
            "JZKH String," +
            "YBKH String," +
            "GRBH_YH String," +
            "JMZH String," +
            "JGDM_S String," +
            "ZJLX String," +
            "JGDM_SQS String," +
            "SYBX String," +
            "HYZK String," +
            "XXDM String," +
            "ZYDM String," +
            "CSNY String," +
            "JGDM String," +
            "BRXB String," +
            "GMYW String," +
            "ZBDYLB String," +
            "DBRM String," +
            "_mc_execute_time BIGINT," +
            "GMS String," +
            "JZRQ String," +
            "JMLX String," +
            "XGSJ String," +
            "ZHONGBAOTPYECODE String," +
            "SBHM String," +
            "LXDH String," +
            "JDSJ String," +
            "SFDM String," +
            "ZXBZ String," +
            "HKDZ_SQS String," +
            "ZXSJ String," +
            "_mc_schema String," +
            "XZZ_DH String," +
            "SRC String," +
            "DWMC String," +
            "ZHONGBAOTYPECODE String," +
            "HKDZ_QTDZ String," +
            "BRXZ String," +
            "FEENUMBER String," +
            "_mc_db_lsn String," +
            "_mc_db_rowid String," +
            "MZDM String," +
            "JDJG String," +
            "ZXR String," +
            "MZHM String," +
            "XZZ_SQS String," +
            "JDR String," +
            "XZZ_YB String," +
            "BRXM String," +
            "EMPIID String," +
            "_mc_sequence String," +
            "LXGX String," +
            "FYZH String," +
            "JZCS String," +
            "RQFL String," +
            "DWXH String," +
            "LXRM String," +
            "SFZH String," +
            "CZRQ String," +
            "JTDH String," +
            "HKDZ_X String," +
            "HKYB String," +
            "ZZTX String," +
            "XZZ_S String," +
            "DBGX String," +
            "XZZ_X String," +
            "HKDZ_S String," +
            "BRID String," +
            "DWDH String," +
            "HKDZ String," +
            "_mc_operation String," +
            "ZZJDBZ String," +
            "CSD_X String," +
            "DWYB String," +
            "CSD_S String" +
            ", et as TO_TIMESTAMP(FROM_UNIXTIME(_mc_execute_time/1000, 'yyyy-MM-dd HH:mm:ss'))," +
            " WATERMARK FOR et AS et - INTERVAL '5' SECOND " +
            ") WITH (" +
            "    'connector' = 'kafka'," +
            "    'topic' = 'ZHYLADMINBSHIS.MS_BRDA'," +
            "    'properties.bootstrap.servers' = '192.168.239.1:9092'," +
            "    'format' = 'json'," +
            "    'properties.group.id'='4dscccuaa',"+
            "'scan.startup.mode' = 'earliest-offset',"+
            " 'json.fail-on-missing-field' = 'false'," +
            " 'json.ignore-parse-errors' = 'true'" +
            ");";



    

}
