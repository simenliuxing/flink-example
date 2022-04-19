package com.dtstack.flink.fk;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

import static com.dtstack.flink.fk.util.GlobalConfig.jaas;
import static com.dtstack.flink.fk.util.GlobalConfig.krb5;

/**
 * 主程序
 *
 * @author xuzhiwen
 */
public class SinkKuduApp2 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("java.security.auth.login.config", "chuixue_jaas.conf");
        System.setProperty("java.security.krb5.conf", "krb5.conf");

//        System.setProperty("java.security.auth.login.config", "/Users/chuixue/dtstack/workspace/flink-example/datastream/hwkafka-to-kudu/src/main/resources/kerberos/chuixue_jaas.conf");
//        System.setProperty("java.security.krb5.conf", "/Users/chuixue/dtstack/workspace/flink-example/datastream/hwkafka-to-kudu/src/main/resources/kerberos/krb5.conf");

        Properties props = new Properties();
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");

        props.put("bootstrap.servers", "172.16.83.204:21013,172.16.83.229:21013,172.16.83.234:21013");
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");

        env.setParallelism(1);
        env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props).setStartFromLatest()).print();

        env.execute("kerberos kafka to lad kudu");
    }
}
