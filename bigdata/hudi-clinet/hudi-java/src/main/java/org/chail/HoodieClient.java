package org.chail;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.common.HoodieExampleDataGenerator;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.internal.schema.action.TableChange;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author lixiang
 * @description hudi-client增删改查
 */
public class HoodieClient {
    public static  List<String> partitionPath = Arrays.asList("20201221","20201222","20201223","20201224","20201225","20201226","20201227");

    private HoodieJavaWriteClient<HoodieAvroPayload> client;

    private String tableFormat;

    /**
     * HDFS 路径
     */
    private final static String DEFAULT_HDFS_PATH = "hdfs://192.168.139.100:8020";

    /**
     * 默认HDFS 存放的路径
     */
    private final static String HIVE_WAREHOUSE = "/user/hive/warehouse";



    private String tablePath;
    private String tableName;

    // ==============================构造方法开始==============================
    public HoodieClient(HiveJdbc.HiveTable hiveTable, HoodieTableType tableType,boolean initTable) {
        this.tableFormat = hiveTable.getTableFormat();
        initHuDiClient(hiveTable.getHdfsUrl()+hiveTable.getTablePath(), hiveTable.getName(), tableFormat, tableType,initTable);
    }


    /**
     * 初始化HoodieJavaWriteClient
     *
     * @param tableName
     * @param tableFormat
     * @param tableType
     */
    private void initHuDiClient(String tablePath, String tableName, String tableFormat, HoodieTableType tableType,boolean initTable) {
        this.tablePath = tablePath;
        this.tableName = tableName;
        // 初始化Hoodie表
        // 创建HDFS路径
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true");
        hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
        try {
            // 检查路径是否存在
            if (initTable) {
                // 初始化Hoodie Table 创建Hoodie表的tablePath，写入初始化元数据信息
                HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(tableType.name())
                        .setTableName(tableName)
                        .setHiveStylePartitioningEnable(true)
                        .setPartitionFields("dt")
                        .setPayloadClassName(HoodieAvroPayload.class.getName())
                        .initTable(hadoopConf, tablePath);
            }
        } catch (IOException e) {
            throw new RuntimeException("初始化表Hoodie表异常," + tableName);
        }
        // 创建write client conf
        HoodieWriteConfig huDiWriteConf = HoodieWriteConfig.newBuilder()
                // 数据schema
                .withSchema(tableFormat)
                .withHiveStylePartitioningEnabled(true)
                // 数据插入更新并行度
                .withParallelism(2, 2)
                // 数据删除并行度
                .withDeleteParallelism(2)
                // HuDi表索引类型，BLOOM
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                // 合并
                .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build())
                //.withEmbeddedTimelineServerEnabled(false)
                .withPath(tablePath)
                .forTable(tableName)
                .build();
//        HiveSyncTool hiveSyncTool = new HiveSyncTool();
        /*HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(tableFormat)
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();*/





        /*huDiWriteConf.getProps().setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(),"table_name");
        huDiWriteConf.getProps().setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(),"uuid");*/

        // 获得HuDi write client
        this.client = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), huDiWriteConf);
    }

    /**
     * 单条插入Hoodie数据
     *
     * @param insertObject
     */
    public void upsertOne(JSONObject insertObject) {
        upsert(Arrays.asList(insertObject),true);
    }

    /**
     * 批量插入Hoodie数据
     *
     * @param insertObjects
     */
    public void upsertBatch(List<JSONObject> insertObject,boolean upserts) {
        upsert(insertObject,upserts);
    }

    public void deleteOne(String primaryKey, String partitionPath) {
        delete(Arrays.asList(primaryKey), partitionPath);
    }

    public void deleteBatch(List<String> primaryKeys, String tableName) {
        delete(primaryKeys, tableName);
    }

    /**
     * 删除逻辑
     *
     * @param primaryKeys
     * @param partitionPath
     */
    private void delete(List<String> primaryKeys, String  partitionPath) {
        String newCommitTime = client.startCommit();
        List<HoodieKey> deleteKeys = primaryKeys.stream().map(key -> new HoodieKey(key, partitionPath)).collect(Collectors.toList());
        client.delete(deleteKeys, newCommitTime);
    }

    /**
     * 新增修改公用操作
     *
     * @param insertObjects
     */
    private void upsert(List<JSONObject> insertObjects,boolean upsert) {
        String newCommitTime = client.startCommit();
        Schema avroSchema = new Schema.Parser().parse(tableFormat);
        List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = insertObjects.stream().map(obj -> {
            GenericRecord genericRecord = new GenericData.Record(avroSchema);
            obj.forEach(genericRecord::put);
            HoodieKey hoodieKey = new HoodieKey(obj.getString("id"),obj.get("dt").toString() );
            HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(genericRecord));
            return (HoodieRecord<HoodieAvroPayload>) new HoodieAvroRecord<>(hoodieKey, payload);
        }).collect(Collectors.toList());
        // 获取upsertStatus
        if(upsert){
            client.insert(hoodieRecords, newCommitTime);
        }else {
            client.upsert(hoodieRecords, newCommitTime);
        }
    }


    void upsert(List<JSONObject> insertObjects, boolean upsert, String schema) {
        String newCommitTime = client.startCommit();
        Schema avroSchema = new Schema.Parser().parse(schema);
        List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = insertObjects.stream().map(obj -> {
            GenericRecord genericRecord = new GenericData.Record(avroSchema);
            obj.forEach(genericRecord::put);
            HoodieKey hoodieKey = new HoodieKey(obj.getString("id"),obj.get("dt").toString() );
            HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(genericRecord));
            return (HoodieRecord<HoodieAvroPayload>) new HoodieAvroRecord<>(hoodieKey, payload);
        }).collect(Collectors.toList());
        // 获取upsertStatus
        if(upsert){
            client.insert(hoodieRecords, newCommitTime);
        }else {
            client.upsert(hoodieRecords, newCommitTime);
        }
    }


    public void ddl(String colName,Schema.Type type, String tableFormat) {
        try {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true");
            hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
            // 创建write client conf
            HoodieWriteConfig huDiWriteConf = HoodieWriteConfig.newBuilder()
                    // 数据schema
                    .withSchema(tableFormat)
                    .withHiveStylePartitioningEnabled(true)
                    // 数据插入更新并行度
                    .withParallelism(2, 2)
                    // 数据删除并行度
                    .withDeleteParallelism(2)
                    // HuDi表索引类型，BLOOM
                    .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                    // 合并
                    .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build())
                    //.withEmbeddedTimelineServerEnabled(false)
                    .withPath(tablePath)
                    .forTable(tableName)
                    .build();

            this.  client = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), huDiWriteConf);

            client.addColumn(colName, Schema.create(type),null, "", TableChange.ColumnPositionChange.ColumnPositionType.AFTER);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public void ddl() {
        try {

            //client.getTableServiceClient().scheduleCompaction(build)
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 客户端关闭方法
     */
    public void close() {
        client.close();
    }


    public static void main(String[] args) {
        //hive创建一个表

        //插入数据















    }




}
