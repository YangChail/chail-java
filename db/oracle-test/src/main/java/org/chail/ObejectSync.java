package org.chail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ObejectSync {


    public static List<String[]> get(Connection connection, String sql){
        Statement statement = null;
        ResultSet resultSet = null;
        List<String[]> res=new ArrayList<>();
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String[] obj = new String[]{
                        resultSet.getString(1)
                        , resultSet.getString(2)
                        , resultSet.getString(3)
                        , resultSet.getString(4)
                };
                res.add(obj);
            }
        } catch (Exception e) {
            System.out.println(sql + "  执行错误" + e.getMessage());
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                System.out.println(sql + "  执行错误" + e.getMessage());
            }
        }
        return res;
    }




    public static void execute(Connection connection, List<String[]> res){
        for (String[] s : res) {
            System.out.println(String.format("正在执行Schema:%s,Type:%s,Object:%s", s[0], s[1], s[2]));
            SqlUtils.execute(connection, s[3]);
        }
    }



}
