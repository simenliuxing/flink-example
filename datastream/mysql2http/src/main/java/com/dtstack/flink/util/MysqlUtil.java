package com.dtstack.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * @author beifeng
 */
public class MysqlUtil {
    private final static Logger LOG = LoggerFactory.getLogger(MysqlUtil.class);
    private static final int RETRY_NUM = 3;

    private static final String DEFAULT_DRIVER = "com.mysql.jdbc.Driver";

    /**
     * 获取连接最多尝试3次
     */
    public static Connection getConnection() {

        try {
            String driver = GlobalConfig.mysqlDriver();
            if (driver == null || "".equals(driver)) {
                driver = DEFAULT_DRIVER;
            }
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            LOG.error("cause : {}", e.getMessage());
            throw new RuntimeException("connection  failure, cause:", e);
        }

        String url = GlobalConfig.mysqlUrl();
        Connection connection = null;
        for (int i = 0; ; i++) {
            try {
                connection = DriverManager.getConnection(
                        url, GlobalConfig.mysqlName(), GlobalConfig.mysqlPwd());
                try (Statement statement = connection.createStatement()) {
                    statement.execute("select 1");
                    break;
                }
            } catch (SQLException e) {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException ignored) {
                        // HE IS NOW NOTHING
                    }
                }
                if (i + 1 == RETRY_NUM) {
                    throw new RuntimeException("The maximum number of attempts to obtain connections," +
                            " max number" + " :" + RETRY_NUM + "\n" + "Cause :" + e);
                } else {
                    try {
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException ignored) {
                        // HE IS NOW NOTHING
                    }
                }
            }
        }

        return connection;
    }

    public static void close(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.error("close failed ,cause:",e);
        }
    }

}
