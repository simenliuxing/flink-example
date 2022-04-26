package com.dtstack.flink.fk;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 主程序
 *
 * @author xuzhiwen
 */
public class FlinkApiHwKerberosKafka {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");

        props.put("bootstrap.servers", "172.16.83.204:21013,172.16.83.229:21013,172.16.83.234:21013");


        env.setParallelism(1);
        env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props).setStartFromEarliest()).print();

        env.execute("kerberos kafka to lad kudu");
    }
}
