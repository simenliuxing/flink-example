package com.dtstack.flink.streamudf.demo.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author xiuyuan
 * @date 2021-12-21 10:30:30
 * @deprecated JDBC连接数据库的工具类
 */
public class ConnectionTool {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionTool.class);

    public static Connection getConnect(String driverName, String url, String user, String password) {
        try {
            Class.forName(driverName);
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException | ClassNotFoundException e) {
            LOG.error("Failed to load MySQL database driver！\n" + e);
            return null;
        }
    }

    /**
     * @param con 将要释放的连接
     * @throws SQLException 释放连接时可能会产生异常
     */
    public static void close(Connection con) throws SQLException {
        if (con != null && !con.isClosed()) {
            con.close();
        }
    }

}
