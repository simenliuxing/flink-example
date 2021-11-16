package com.dtstack.flink.demo.util;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * kafka相关的工具类
 *
 * @author beifeng
 */
public class KafkaUtil {
    public static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers());
        props.setProperty("group.id", GlobalConfigUtil.groupId());
        props.setProperty("key.serializer", GlobalConfigUtil.kks());
        props.setProperty("key.deserializer", GlobalConfigUtil.kkd());
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return props;
    }
}
