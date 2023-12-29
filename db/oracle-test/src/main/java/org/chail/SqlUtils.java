package org.chail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlUtils {


    public static Object getOne(Connection connection, String sql) {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                return resultSet.getObject(1);
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
        return null;
    }




    public static void execute(Connection connection, String sql) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } catch (Exception e) {
            System.out.println(sql + "  执行错误" + e.getMessage());
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                System.out.println(sql + "  执行错误" + e.getMessage());
            }
        }
    }

}
