package com.dtstack.flink.fk;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * 主程序
 *
 * @author xuzhiwen
 */
public class KafkaApiHwKerberosKafka {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.addSource(new KafkaSouceFun()).print();

        env.execute("kerberos kafka to lad kudu");
    }
}

class KafkaSouceFun extends RichSourceFunction<String> {
    protected static final Logger LOG = LoggerFactory.getLogger(KafkaSouceFun.class);
    KafkaConsumer<String, String> consumer = null;

    @Override
    public void open(Configuration parameters) {
        System.setProperty("java.security.krb5.conf", "krb5.conf");

        Properties props = new Properties();
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        // todo 注意keyTab=路径，容器中直接些相对路径
        props.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" +
                "useKeyTab=true\n" +
                "keyTab=\"kafka.keytab\"\n" +
                "principal=\"kafka/hadoop.hadoop.com@HADOOP.COM\"\n" +
                "storeKey=true\n" +
                "debug=false\n" +
                "useTicketCache=false;");

        props.put("bootstrap.servers", "172.16.83.204:21013,172.16.83.229:21013,172.16.83.234:21013");
        props.put("group.id", "test22222222222");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
    }

    @Override
    public void run(SourceContext<String> ctx) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                ctx.collect(record.value());
            }
        }
    }

    @Override
    public void cancel() {

    }
}
