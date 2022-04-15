package com.dtstack.flink.fk.util;

import java.util.Properties;

import static com.dtstack.flink.fk.util.GlobalConfig.*;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * kafka配置工具
 *
 * @author beifeng
 */
public class KafkaConfig {
  static {
    // 加载kerberos到环境变量
    System.setProperty("java.security.auth.login.config", jaas());
    System.setProperty("java.security.krb5.conf", krb5());
  }

  public static Properties getKafkaProperties() {

    Properties props = new Properties();
    props.put("security.protocol", "SASL_PLAINTEXT");
    props.put("sasl.kerberos.service.name", "kafka");
    props.put("sasl.mechanism", "GSSAPI");

    // -----------------------------------------------------

    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(GROUP_ID_CONFIG, groupId());
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, kkd());
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, kkd());
    props.put(KEY_SERIALIZER_CLASS_CONFIG, kks());
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, kks());
    return props;
  }
}
