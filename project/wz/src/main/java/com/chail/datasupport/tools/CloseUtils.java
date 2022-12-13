package com.chail.datasupport.tools;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.sql.*;

/**
 * @author Xing Gang
 * @date 2021/4/24 2:26 PM
 */
public class CloseUtils {
    public static final Logger log = LogManager.getLogger(CheckJob.class);
    public static void closeQuietly(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException se) {
                log.error("close result set exception", se);
            }
        }
    }

    public static void closeQuietly(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException se) {
                log.error("close result set exception", se);
            }
        }
    }


    public static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.error("close connection exception ", e);
            }
        }
    }

    public static void closeQuietly(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (Exception e) {
                log.error("close prepared preparedStatement exception", e);
            }
        }
    }

    /**
     * 关闭
     *
     * @param closeObjects
     */
    public static void close(AutoCloseable... closeObjects) {
        for (AutoCloseable obj : closeObjects) {
            if (obj != null) {
                try {
                    obj.close();
                } catch (Exception e) {
                    log.error("close prepared statement exception", e);
                }
            }
        }
    }


    public static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.error("close exception", e);
            }
        }
    }

}
