package org.chail;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;

import java.util.*;

import static org.apache.hudi.hive.HiveSyncConfig.*;
import static org.chail.HoodieClient.partitionPath;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class HudiTest {

   public static final String ip="192.168.42.54";

    public static  Set<String> partitionPathSet = new HashSet<>(partitionPath.size());

    //public static final String ip="192.168.239.223";

    public static void main(String[] args) throws InterruptedException {
       HiveJdbc jdbc=new HiveJdbc(ip,"10000","chail","hive","hive");

        System.setProperty("HADOOP_USER_NAME", "hive");
        HiveJdbc.HiveTable table = HiveJdbc.HiveTable.builder().schema("chail")
                .name("test_table")
                .hdfsUrl("hdfs://192.168.42.54:9000")
                .tablePath("/user/hive/warehouse/chail.db/test_table")
               .partition(HiveJdbc.HiveTableColumn.builder().name("dt").type(Schema.Type.STRING.getName()).build())
                .columns(new ArrayList<>(Arrays.asList(HiveJdbc.HiveTableColumn.builder().name("id").type(Schema.Type.INT.getName()).build()
                        , HiveJdbc.HiveTableColumn.builder().name("username").type(Schema.Type.STRING.getName()).build()
                        , HiveJdbc.HiveTableColumn.builder().name("password").type(Schema.Type.STRING.getName()).build()
                        , HiveJdbc.HiveTableColumn.builder().name("age").type(Schema.Type.INT.getName()).build())))
                .build();
        jdbc.createTable(table);
        HoodieClient hoodieClient= new HoodieClient(table, HoodieTableType.MERGE_ON_READ,true);
        List<JSONObject> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JSONObject json = new JSONObject();
            json.put("id",i);
            json.put("username","user_"+new Random().nextInt(9000));
            json.put("password", "abc_"+new Random().nextInt(1000));
            json.put("age", new Random().nextInt(10));
            String s = partitionPath.get(new Random().nextInt(partitionPath.size() - 1));
            partitionPathSet.add(s);
            json.put("dt",s );
            list.add(json);
        }
        hoodieClient.upsertBatch(list,false);

//        Thread.sleep(60000);
        list.clear();
        JSONObject json = new JSONObject();
        json.put("id",1);
        json.put("username","user_AAA");
        json.put("password", "abc_AAA");
        json.put("age", new Random().nextInt(10));
        String s = partitionPath.get(new Random().nextInt(partitionPath.size() - 1));
        partitionPathSet.add(s);
        json.put("dt",  s);
        list.add(json);
        hoodieClient.upsertBatch(list,true);

//        Thread.sleep(60000);
        //hoodieClient.deleteOne("2","");




        jdbc.addPartition(table, new ArrayList<>(partitionPathSet));

        ddl(jdbc,json,table,hoodieClient);


        jdbc.addPartition(table, new ArrayList<>(partitionPathSet));

//        Properties props=new Properties();
//        props.put(HIVE_URL.key(), "jdbc:hive2://="+ip+":10000");
//        props.put(HIVE_USER.key(),"hive");
//        props.put(HIVE_PASS.key(), "hive");
//        props.put("--target-base-path",table.getHdfsUrl()+table.getTablePath());







//        HiveSyncTool hiveSyncTool = new HiveSyncTool(props, new Configuration());
//        hiveSyncTool.syncHoodieTable();

    }


    private static void ddl(HiveJdbc jdbc,JSONObject json, HiveJdbc.HiveTable table,HoodieClient hoodieClient ){
        table.getColumns().add( HiveJdbc.HiveTableColumn.builder().name("add_string").type(Schema.Type.STRING.getName()).build());
        String tableFormat = table.getTableFormat();
        Schema avroSchema = new Schema.Parser().parse(tableFormat);
        int pos = avroSchema.getField("add_string").pos();

        hoodieClient.ddl("add_string", Schema.Type.STRING,tableFormat,pos+"");
        List<JSONObject> list = new ArrayList<>();
        String s = partitionPathSet.stream().findFirst().get();
        //partitionPathSet.add(s);
        list.add(new JSONObject().fluentPut("id",111).fluentPut("username","user_AAA11").fluentPut("password", "abc_AAA1").fluentPut("add_string", "add_string1").fluentPut("age", new Random().nextInt(10)).fluentPut("dt", s));
        list.add(new JSONObject().fluentPut("id",222).fluentPut("username","user_AAA222").fluentPut("password", "abc_AAA2").fluentPut("add_string", "add_string2").fluentPut("age", new Random().nextInt(10)).fluentPut("dt", s));
        hoodieClient.upsert(list,true,tableFormat);

         s ="43301226";
        partitionPathSet.add(s);
//
//
        list.add(new JSONObject().fluentPut("id",111).fluentPut("username","user_AAA11").fluentPut("password", "abc_AAA1").fluentPut("add_string", "add_string1").fluentPut("age", new Random().nextInt(10)).fluentPut("dt", s));
        list.add(new JSONObject().fluentPut("id",222).fluentPut("username","user_AAA222").fluentPut("password", "abc_AAA2").fluentPut("add_string", "add_string2").fluentPut("age", new Random().nextInt(10)).fluentPut("dt", s));
        hoodieClient.upsert(list,true,tableFormat);

        jdbc.ddl(table, "add_string",Schema.Type.STRING.name());

        jdbc.alterSpark(table);




    }
}