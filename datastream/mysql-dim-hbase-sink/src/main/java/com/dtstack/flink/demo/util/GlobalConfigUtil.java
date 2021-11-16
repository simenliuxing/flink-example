package com.dtstack.flink.demo.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 用于获取application.conf配置信息的工具类
 *
 * @author beifeng
 */
public class GlobalConfigUtil {
    /**
     * 默认读取application的配置文件
     */
    private static final Config CONFIG = ConfigFactory.load("application.conf");


    /**
     * kafka相关配置
     */
    public static String bootstrapServers() {
        return CONFIG.getString("bootstrap.servers");
    }

    public static String groupId() {
        return CONFIG.getString("group.id");
    }

    public static String zkHp() {
        return CONFIG.getString("zookeeper.connect");
    }

    public static String kks() {
        return CONFIG.getString("key.serializer");
    }

    public static String kkd() {
        return CONFIG.getString("key.deserializer");
    }

    /**
     * 输入的kafka topic
     */
    public static String getTopic() {
        return CONFIG.getString("input.topic");
    }


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

}