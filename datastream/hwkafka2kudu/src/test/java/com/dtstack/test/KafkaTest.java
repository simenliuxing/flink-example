package com.dtstack.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import static com.dtstack.flink.fk.util.GlobalConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaTest {
  public static void main(String[] args) {

    System.setProperty("java.security.auth.login.config", jaas());
    System.setProperty("java.security.krb5.conf", krb5());
    Properties props = new Properties();

    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, kkd());
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, kkd());
    props.put(KEY_SERIALIZER_CLASS_CONFIG, kks());
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, kks());

    props.put("bootstrap.servers", "172.16.83.204:21013,172.16.83.229:21013,172.16.83.234:21013");
    props.put("group.id", "test");
    props.put("auto.offset.reset", "earliest");

    props.put("security.protocol", "SASL_PLAINTEXT");
    props.put("sasl.mechanism", "GSSAPI");
    props.put("sasl.kerberos.service.name", "kafka");

    new Thread(() -> comsumer(props)).start();

    final Scanner scanner = new Scanner(System.in);
    KafkaProducer<String, String> prod = new KafkaProducer<>(props);
    while (scanner.hasNext()) {
      final String l = scanner.nextLine();
      if (l.length() < 10) {
        continue;
      }

      prod.send(
          new ProducerRecord<>("test", l),
          (recordMetadata, e) -> {
            if (e != null) {
              System.out.println("发送失败");
            } else {
              System.out.println("发送成功");
            }
          });
      prod.flush();
    }

    // comsumer(props);
  }

  private static void comsumer(Properties props) {
    KafkaConsumer kafkaConsumer = new KafkaConsumer<>(props);
    kafkaConsumer.subscribe(Arrays.asList("test"));
    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(1);
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(
            "Partition: "
                + record.partition()
                + " Offset: "
                + record.offset()
                + " Value: "
                + record.value()
                + " ThreadID: "
                + Thread.currentThread().getId());
      }
    }
  }
}
