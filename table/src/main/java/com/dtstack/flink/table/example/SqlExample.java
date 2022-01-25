package com.dtstack.flink.table.example;

import akka.actor.SupervisorStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @Author xiaoyu
 * @Create 2022/1/25 11:38
 * @Description
 */
public class SqlExample {
  public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings
              .newInstance()
              .useBlinkPlanner()
              .inStreamingMode()
              .build());


      //设置每1分钟开启一次检查点
      env.enableCheckpointing(60000);
      //设置两个检查点时间间隔1分钟
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000);
      //设置检查点的超时时间10分钟
      env.getCheckpointConfig().setCheckpointTimeout(600000);
      //6分钟内重启3次 每次重启间隔10s
      env.setRestartStrategy(
              RestartStrategies.failureRateRestart(3, Time.of(6, TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)));

      //{"user_id":2,"user_name":"lisi","age":18,"status":true}
      tableEnv.executeSql(
        "CREATE TABLE KafkaTable (\n"
            + "  `user_id` BIGINT,\n"
            + "  `user_name` string,\n"
            + "  `age` int,\n"
            + "  `status` BOOLEAN\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = 'xy_cs_tableSql',\n"
            + "  'properties.bootstrap.servers' = '172.16.100.109:9092',\n"
            + "  'properties.group.id' = 'xy_testGroup',\n"
            + "  'scan.startup.mode' = 'earliest-offset',\n"
            + "  'format' = 'json'\n"
            + ")");

      tableEnv.executeSql(
              "CREATE TABLE MyUserTable (\n" +
              "  id BIGINT,\n" +
              "  name STRING,\n" +
              "  age INT,\n" +
              "  status BOOLEAN,\n" +
              "  PRIMARY KEY (id) NOT ENFORCED\n" +
              ") WITH (\n" +
              "   'connector' = 'jdbc',\n" +
              "   'url' = 'jdbc:mysql://192.168.114.44:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false',\n" +
              "   'table-name' = 'sinkFlink12',\n" +
              "   'username' = 'root',\n" +
              "   'password' = 'root'\n" +
              ")");

      tableEnv.executeSql("insert into MyUserTable select * from KafkaTable");

      env.execute("sqlExample");
  }
}
