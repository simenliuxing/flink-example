package com.dtstack.flink.demo.util;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 此程序用于生成数据发送kafka
 *
 * @author beifeng
 */
public class DataProducer {
    private static final String[] USER_IDS = {
            "xzw", "xzw2", "xzw3", "abc", "dsa", "asd", "asd2", "ddd", "aaa", "sda", "dsa", "sad", "dwe", "shuai", "xzw-smart",
            "flink", "edgnb", "egdNb", "edg-nb", "edg-nbnb", "asddd", "edg-zhennb", "ada1b", "bda", "sdasd", "Gfgf", "Dfdf"
    };
    static AtomicInteger i = new AtomicInteger(0);

    public static void main(String[] args) {
        System.out.println(USER_IDS.length);
        for (int j = 0; j < 5; j++) {
            new Thread(() -> {
                producer("test1");
            }).start();
        }

        //producer("test1",System.in);
    }

    public static void producer(String topic, InputStream inputStream) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProp());
        Scanner scanner = new Scanner(inputStream);
        Random random = new Random();
        while (true) {
            int i = scanner.nextInt();
            if (i > 0) {
                User user = new User(USER_IDS[random.nextInt(USER_IDS.length + 1)], random.nextInt(100),
                        System.currentTimeMillis() - random.nextInt(1000), random.nextInt(11));
                String jsonStr = JSON.toJSONString(user);
                producer.send(new ProducerRecord<>(topic, jsonStr));
                producer.flush();
            } else {
                break;
            }
        }
    }

    public static void producer(String topic) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProp());
        System.out.println("start ---------" + i.incrementAndGet());
        Random random = new Random();
        for (; ; ) {
            User user = new User(USER_IDS[random.nextInt(USER_IDS.length)], random.nextInt(100),
                    System.currentTimeMillis() - random.nextInt(1000), random.nextInt(10));
            String jsonStr = JSON.toJSONString(user);
            producer.send(new ProducerRecord<>(topic, jsonStr));
            producer.flush();
//            try {
//                TimeUnit.SECONDS.sleep(1);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    public static Properties getProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.100.109:9092");
        props.put("acks", "0");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "xzw");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    static class User {
        String userid;
        int age;
        long dttime;
        int cityId;

        public User(String userid, int age, long dttime, int cityId) {
            this.userid = userid;
            this.age = age;
            this.dttime = dttime;
            this.cityId = cityId;
        }

        public String getUserid() {
            return userid;
        }

        public void setUserid(String userid) {
            this.userid = userid;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public long getDttime() {
            return dttime;
        }

        public void setDttime(long dttime) {
            this.dttime = dttime;
        }

        public int getCityId() {
            return cityId;
        }

        public void setCityId(int cityId) {
            this.cityId = cityId;
        }
    }

}



