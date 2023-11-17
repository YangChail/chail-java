package org.chail;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DropTablesInSchemas {

    // replace with your connection details
    private static final String DB_URL = "jdbc:oracle:thin:@//10.88.2.159:1521/xe";
    private static final String DB_USER = "mc_qy";
    private static final String DB_PASSWORD = "Mc_qy2023";

    // replace with your schemas

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<String> SCHEMAS = readLine();
        System.out.println("获取到shema数量"+SCHEMAS.size());
        SCHEMAS = SCHEMAS.stream().distinct().collect(Collectors.toList());
        AtomicInteger size = new AtomicInteger(SCHEMAS.size());
        for (String schema : SCHEMAS) {
            executorService.submit(() -> {
                dropTablesInSchema(schema);
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

    private static void dropTablesInSchema(String schema) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT table_name FROM all_tables WHERE owner = '" + schema + "'");
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                Statement dropStatement =null;
                try {
                     dropStatement = connection.createStatement();
                    dropStatement.execute("DROP TABLE " + schema + "." + tableName);
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }finally {
                    if (dropStatement != null) {
                        dropStatement.close();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }finally {
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
                System.out.println(schema+"  删除错误"+e.getMessage());
            }
        }
    }


    public  static List<String> readLine() {
        List<String> lines = new ArrayList<>();
        try {
            // Read the file from the resources folder
            InputStream inputStream = DropTablesInSchemas.class.getResourceAsStream("/schema.txt");
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