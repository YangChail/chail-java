package org.chail;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Builder;
import lombok.Data;
import org.apache.avro.Schema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class HiveJdbc {

    private static final String SYNC_PARTITION_SQL="ALTER TABLE ${tableName} ADD IF NOT EXISTS PARTITION (${partition_field}='${partition_value}') LOCATION '${table_path}/${partition_value}'";
    static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private Connection conn;

    private static final String DDL="ALTER TABLE ${tableName} ADD COLUMNS (${colName} ${colType})";

    public HiveJdbc(String host, String port, String database, String user, String password) {
        String DB_URL = String.format("jdbc:hive2://%s:%s/%s", host, port, database);
        this.conn = null;
        Statement stmt = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addPartition(HiveTable table,List <String> partitionList) {

        for (String partition : partitionList) {
            String sql = SYNC_PARTITION_SQL.replace("${tableName}", table.getName()).replace("${partition_field}", table.getPartition().getName())
                    .replace("${partition_value}", partition).replace("${table_path}", table.getTablePath());

            Statement stmt = null;
            try {
                // Create a statement
                stmt = conn.createStatement();
                stmt.execute(sql);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (stmt != null) stmt.close();
                } catch (SQLException se2) {
                }
            }
        }

    }


    public void ddl(HiveTable table, String colName, String type) {
        Statement stmt = null;
        try {
            // Create a statement
            stmt = conn.createStatement();
            stmt.execute(DDL.replace("${tableName}", table.getName()).replace("${colName}", colName).replace("${colType}", type));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            }
        }

    }

    public static String TBLPROPERTIES_VALUE="ALTER TABLE ${tableName} SET TBLPROPERTIES ('${pro_key}' = '${pro_value}')";

    public void alterSpark(HiveTable table) {
        Statement stmt = null;
        try {
            // Create a statement
            String sparkTableFormat = table.getSparkTableFormat();
            stmt = conn.createStatement();
            stmt.execute(TBLPROPERTIES_VALUE.replace("${tableName}", table.getName()).replace("${pro_value}", sparkTableFormat).replace("${pro_key}", "spark.sql.sources.schema"));

            stmt.execute(TBLPROPERTIES_VALUE.replace("${tableName}", table.getName()).replace("${pro_value}", System.currentTimeMillis()+"").replace("${pro_key}", "transient_lastDdlTime"));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            }
        }

    }






    public void createTable(HiveTable table) {
        String sql = "CREATE  TABLE `${schemaName}`.`${tableName}`(\n" +
                "  `_hoodie_commit_time` string, \n" +
                "  `_hoodie_commit_seqno` string, \n" +
                "  `_hoodie_record_key` string, \n" +
                "  `_hoodie_partition_path` string, \n" +
                "  `_hoodie_file_name` string, \n" +
                "${column}\n" +
                ")\n" +
                "${paritition}\n"+
                "ROW FORMAT SERDE \n" +
                "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \n" +
                "WITH SERDEPROPERTIES ( \n" +
                "  'path'='${tablePath}') \n" +
                "STORED AS INPUTFORMAT \n" +
                "  'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat' \n" +
                "OUTPUTFORMAT \n" +
                "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n" +
                "LOCATION\n" +
                "  '${hdfsUrl}${tablePath}'\n" +
                "TBLPROPERTIES (\n" +
                "  'spark.sql.sources.provider'='hudi', \n" +
                "  'spark.sql.sources.schema.numPartCols'='1', \n" +
                "  'spark.sql.sources.schema.partCol.0'='dt', \n" +
                "  'spark.sql.sources.schema'='${spark_schema}',\n" +
                "  'spark.sql.sources.schema.numParts'='1',\n" +
                " 'type'='mor'\n" +
                //"  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"volume\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"symbol\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"year\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"month\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"high\",\"type\":\"double\",\"nullable\":false,\"metadata\":{}},{\"name\":\"low\",\"type\":\"double\",\"nullable\":false,\"metadata\":{}},{\"name\":\"key\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"close\",\"type\":\"double\",\"nullable\":false,\"metadata\":{}},{\"name\":\"open\",\"type\":\"double\",\"nullable\":false,\"metadata\":{}},{\"name\":\"day\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}', \n" +
                ")";
        sql = sql.replace("${schemaName}", table.getSchema());
        sql = sql.replace("${tableName}", table.getName());
        sql = sql.replace("${tablePath}", table.getTablePath());
        sql = sql.replace("${hdfsUrl}", table.getHdfsUrl());
        sql = sql.replace("${spark_schema}", table.getSparkTableFormat());
        sql = sql.replace("${column}", table.getColumns().stream().map(e -> String.format("`%s` %s", e.getName(), e.getType())).collect(Collectors.joining(",\n")));


        if(table.getPartition()!=null){
            sql = sql.replace("${paritition}", String.format("partitioned by (%s %s)", table.getPartition().getName(), table.getPartition().getType()));
        }

        Statement stmt = null;
        try {
            // Create a statement
            stmt = conn.createStatement();
            stmt.execute(String.format("drop table  IF EXISTS %s.%s",table.getSchema(),table.getName()));
            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            }
        }


    }


    public void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Data
    @Builder
    static class HiveTable {
        private String name;
        private String schema;
        private String tablePath;
        private String hdfsUrl;
        private List<HiveTableColumn> columns;
        private HiveTableColumn partition;
        public String getSchemaTableName(){
            return String.format("%s.%s",schema,name);
        }

        public  String getTableFormat(){
            JSONArray fields = new JSONArray();
            for (HiveTableColumn column : columns) {
                JSONObject field = new JSONObject();
                field.put("name",column.getName());
                field.put("type",new JSONArray().fluentAdd(column.getType()).fluentAdd("null"));
                fields.add(field);
            }
            fields.add(new JSONObject().fluentPut("name","dt").fluentPut("type","string"));
            JSONObject schema = new JSONObject();
            schema.put("type","record");
            schema.put("name",name);
            schema.put("fields",fields);
            return schema.toJSONString();
        }

        public  String getSparkTableFormat(){
            JSONArray fields = new JSONArray();
            String[] hudiCol=new String[]{"_hoodie_commit_time","_hoodie_commit_seqno","_hoodie_record_key","_hoodie_partition_path","_hoodie_file_name"};
            for (String col : hudiCol) {
                JSONObject field = new JSONObject();
                field.put("name",col);
                field.put("type","string");
                field.put("nullable",true);
                field.put("metadata",new JSONObject());
                fields.add(field);
            }
            for (HiveTableColumn column : columns) {
                JSONObject field = new JSONObject();
                field.put("name",column.getName());
                field.put("type",column.getType().equals("int")?"integer":column.getType());
                field.put("nullable",true);
                field.put("metadata",new JSONObject());
                fields.add(field);
            }

            fields.add(new JSONObject().fluentPut("name","dt").fluentPut("type","string").fluentPut("nullable",true).fluentPut("metadata",new JSONObject()));
            JSONObject schema = new JSONObject();
            schema.put("type","struct");
            schema.put("fields",fields);
            return schema.toJSONString();
        }
    }

    @Data
    @Builder
    static class HiveTableColumn {
        private String name;
        private String type;
    }


}
