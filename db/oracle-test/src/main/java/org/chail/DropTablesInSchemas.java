package org.chail;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.chail.Compare.getTableId;

public class DropTablesInSchemas {


    private static final String DB_URL0 = "jdbc:oracle:thin:@//10.89.2.41:1521/CZSB";
    private static final String DB_USER0 = "mc_qy";
    private static final String DB_PASSWORD0 = "Mc_qy2023";


    private static final String DB_URL = "jdbc:oracle:thin:@//10.89.2.179:1521/DWHSDB";
    private static final String DB_USER = "mc_qy";
    private static final String DB_PASSWORD = "Mc_qy2023";



    private static final String DB_URL2 = "jdbc:oracle:thin:@//10.88.2.159:1521/ZHYS";
    private static final String DB_USER2 = "mc_qy";
    private static final String DB_PASSWORD2 = "Mc_qy2023";




//    private static final String DB_URL = "jdbc:oracle:thin:@//192.168.52.173:1521/ora11g";
//    private static final String DB_USER = "chail";
//    private static final String DB_PASSWORD = "chail";
//
//
//
//    private static final String DB_URL2 = "jdbc:oracle:thin:@//127.0.0.1:1521/xe";
//    private static final String DB_USER2 = "chail";
//    private static final String DB_PASSWORD2 = "chail";



    static final String com_pare_log="compare.log";

    static final String com_pare_res="compare_res.log";

    static boolean useBig = false;
    static String fileName="/schema2.txt";
    // replace with your schemas

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        if(System.getProperty("use.big","false").equalsIgnoreCase("true")){
            useBig=true;
        }
        if(useBig){
            fileName="/schema.txt";
        }
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Class.forName("org.postgresql.Driver");
        Compare.createtable(useBig);
        WriteMessegeToLog.clear(com_pare_log);
        WriteMessegeToLog.clear(com_pare_res);
        long startTime = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        List<String> SCHEMAS = readLine(fileName);
        System.out.println("获取到shema数量"+SCHEMAS.size());
        SCHEMAS = SCHEMAS.stream().distinct().collect(Collectors.toList());
        AtomicInteger size = new AtomicInteger(SCHEMAS.size());
        for (String schema : SCHEMAS) {
            executorService.submit(() -> {
                //dropTablesInSchema(schema);
                count(schema);
                //doObeject(schema);
                size.getAndDecrement();
            });
        }

        try {
            executorService.shutdown();
            boolean loop;
            do {
                loop = !executorService.awaitTermination(10, TimeUnit.SECONDS);
                System.out.println((String.format("----------------------耗时%sS-剩余%s......",(System.currentTimeMillis()-startTime)/1000,size)));
            } while (loop);
            executorService.shutdownNow();
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

private static final String sql="SELECT\n" +
        "\tOWNER ,\n" +
        "\tOBJECT_TYPE,\n" +
        "\tOBJECT_NAME AS name,\n" +
        "\tDBMS_METADATA.GET_DDL('%s',OBJECT_NAME,OWNER) TEXT\n" +

        "FROM\n" +
        "\tdba_OBJECTS\n" +
        "WHERE\n" +
        "\tOBJECT_TYPE = '%s'\n" +
        "\tAND OWNER ='%s'";
private static final String job_sql="\tSELECT JOB_CREATOR,JOB_NAME, JOB_TYPE ,dbms_metadata.get_ddl('PROCOBJ', job_name) FROM user_scheduler_jobs WHERE JOB_CREATOR='%s'\n";

    private static void doObeject(String schema){
        //schema="CHAIL";
        Connection connection =null;
        Connection connection2 =null;
        String[] objectTypes = { "VIEW",  "PROCEDURE", "FUNCTION",  "SEQUENCE", "PACKAGE","MATERIALIZED_VIEW"};
        try {
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            connection2 = DriverManager.getConnection(DB_URL2, DB_USER2, DB_PASSWORD2);
            for (String objectType : objectTypes) {
                String sqls=String.format(sql,objectType,objectType,schema);
                if("JOB".equals(objectType)){
                    sqls=String.format(job_sql,schema);
                }
                List<String[]> strings = ObejectSync.get(connection, sqls);
                ObejectSync.execute(connection2,strings);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {

                if (connection != null) {
                    connection.close();
                }
                if (connection2 != null) {
                    connection2.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println(schema + "  删除错误" + e.getMessage());
            }
        }



    }

    private static List<String> getAllTables(Connection connection,String schema){
        List<String> allTables=new ArrayList<>();
        String allTableSql="SELECT table_name FROM all_tables WHERE owner = '%s'";
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(String.format(allTableSql,schema));
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                allTables.add(tableName);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                System.out.println(schema + "  删除错误" + e.getMessage());
            }
        }
        return allTables;
    }


    private static void count(String schema) {
        Connection connection =null;
        Connection connection2 =null;
        try {
            if(useBig){
                connection = DriverManager.getConnection(DB_URL0, DB_USER0, DB_PASSWORD0);
            }else{
                connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            }
            connection2 = DriverManager.getConnection(DB_URL2, DB_USER2, DB_PASSWORD2);
            List<String> allTables = getAllTables(connection, schema);
            doCount(schema, allTables, connection, connection2);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
                if (connection2 != null) {
                    connection2.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println(schema + "  删除错误" + e.getMessage());
            }
        }

    }


    private static void doCount(String schema, List<String>allTables,Connection connection,Connection connection2 ){
        String count="select count (*) from %s.%s";
        String resStr="%s.%s--S:%s,T:%s";
        for (String allTable : allTables) {
            try {
                connection.setNetworkTimeout(command -> {
                    throw new RuntimeException("超时");
                },1000*60*20);
                connection2.setNetworkTimeout(command -> {
                    throw new RuntimeException("超时");
                },1000*60*20);
                Object ss = SqlUtils.getOne(connection, String.format(count, schema, allTable));
                Object tt = SqlUtils.getOne(connection2, String.format(count, schema, allTable));
                String sTr = "0";
                String tTr = "0";
                if (ss != null) {
                    sTr = ss.toString();
                }
                if (tt != null) {
                    tTr = tt.toString();
                }
                if (!sTr.equals(tTr)) {
                    WriteMessegeToLog.writeToLog(String.format(resStr, schema, allTable, sTr, tTr), com_pare_log);
                    Integer TNum = Integer.valueOf(tTr);
                    Integer SNum = Integer.valueOf(sTr);
                    if(TNum>0&&SNum>0&&(TNum/SNum==2||TNum/SNum==3||TNum/SNum==4)){
                        repair(connection2,schema,allTable);
                    }else if(Math.abs(SNum-TNum)>0){
                        int tableId = Compare.getTableId(schema, allTable);
                        if(tableId>0){
                            Compare.insertTable(tableId);
                            WriteMessegeToLog.writeToLog(String.format("%s.%s-InsertCompare", schema, allTable), com_pare_log);
                        }
                    }
                }

            }catch (Exception e){
                e.printStackTrace();
                System.out.println(schema +"."+allTable+"  统计错误" + e.getMessage());
            }
        }
    }

        /**
     +     * 修复数据库中指定的模式和表。
     +     *
     +     * @param  connection  数据库连接
     +     * @param  schema      模式名称
     +     * @param  table       表名称
     +     */
    private static void repair(Connection connection,String schema,String table){
        String sql="SELECT \"ods_insert_time\" FROM ( SELECT t.*, ROW_NUMBER() OVER ( ORDER BY \"ods_insert_time\") AS rn, COUNT(*) OVER () AS total_rows FROM %s.%s t ) WHERE rn = total_rows / 2";
        String delSql="DELETE FROM %s.%s  WHERE \"ods_insert_time\" > TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF3')";
        sql = String.format(sql, schema, table);
        Object one = SqlUtils.getOne(connection, sql);
        if (one != null) {
            SqlUtils.execute(connection,String.format(delSql, schema, table, one));
            WriteMessegeToLog.writeToLog("修复表格 "+schema+"."+table, com_pare_log);
        }
    }


    public static boolean isNumeric(String str) {
        if (str == null||"".equals(str)) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (Character.isDigit(str.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }


    private static void dropTablesInSchema(String schema) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        String []selectSql=new String[]{"SELECT MVIEW_NAME FROM all_mviews WHERE owner = '%s'","SELECT table_name FROM all_tables WHERE owner = '%s'"};
        String []droSql=new String[]{"DROP MATERIALIZED VIEW %s.%s","DROP TABLE %s.%s CASCADE CONSTRAINTS"};
        for (int i = 0; i < selectSql.length; i++) {
            try {
                connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                statement = connection.createStatement();
                resultSet = statement.executeQuery(String.format(selectSql[i],schema));
                while (resultSet.next()) {
                    String tableName = resultSet.getString(1);
                    Statement dropStatement = null;
                    try {
                        dropStatement = connection.createStatement();
                        dropStatement.execute(String.format(droSql[i],schema,tableName));
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    } finally {
                        if (dropStatement != null) {
                            dropStatement.close();
                        }
                    }
                }

            } catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
                try {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    if (statement != null) {
                        statement.close();
                    }
                    if (connection != null) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    System.out.println(schema + "  删除错误" + e.getMessage());
                }
            }
        }
    }


    public  static List<String> readLine(String fileName) {
        List<String> lines = new ArrayList<>();
        try {
            // Read the file from the resources folder
            InputStream inputStream = DropTablesInSchemas.class.getResourceAsStream(fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            // Read each line and store it in the list
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            // Close the reader
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return lines;
    }

}