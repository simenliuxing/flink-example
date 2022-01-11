package com.dtstack.flink.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 配置信息
 *
 * @author beifeng
 */
public class GlobalConfig {
    private static final Config CONFIG = ConfigFactory.load("application.conf");

    /**
     * mysql相关配置
     */
    public static String mysqlIp() {
        return CONFIG.getString("mysql.server.ip");
    }

    public static String mysqlPort() {
        return CONFIG.getString("mysql.server.port");
    }

    public static String mysqlDb() {
        return CONFIG.getString("mysql.server.database");
    }

    public static String mysqlName() {
        return CONFIG.getString("mysql.server.username");
    }

    public static String mysqlPwd() {
        return CONFIG.getString("mysql.server.password");
    }

    public static String mysqlUrl() {
        return CONFIG.getString("mysql.url");
    }

    public static String mysqlDriver() {
        return CONFIG.getString("mysql.server.driver");
    }

    public static String getRemoteIp() {
        return CONFIG.getString("remote.server.ip");
    }

    public static String getRemotePort() {
        return CONFIG.getString("remote.server.port");
    }

    public static long getCheckpointInterval() {
        return CONFIG.getLong("checkpoint.interval");
    }
}
