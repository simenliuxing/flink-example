package com.dtstack.flink.table.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author xiaoyu
 * @Create 2022/1/25 10:07
 * @Description
 */
public class TableExample {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,fsSettings);

    env.setParallelism(1);
    //每10s开启一次checkpoint
    env.enableCheckpointing(10000);
    //设置两个检查点时间间隔1分钟
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000);
    //设置检查点超时时间10分钟
    env.getCheckpointConfig().setCheckpointTimeout(600000);

    //6分钟内可以重启3次 每次c重启间隔10s
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(6, TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)));

    //{"user_id":1,"user_name":"zhangsan","score":100.0}
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers","172.16.100.109:9092");
    properties.setProperty("group.id","xy_cs");
    FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>("xy_cs_Table",new SimpleStringSchema(),properties);

    DataStreamSource<String> dataStreamSource = env.addSource(flinkKafkaConsumer);
    SingleOutputStreamOperator<Tuple3<Integer,String,Double>> map = dataStreamSource.map(new MapFunction<String, Tuple3<Integer,String,Double>>() {
      @Override
      public Tuple3<Integer,String,Double> map(String value) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(value);
        return new Tuple3<>(
                jsonNode.findValue("user_id").asInt(),
                jsonNode.findValue("user_name").asText(),
                jsonNode.findValue("score").asDouble());
      }
    });




    Table table = tableEnv.fromDataStream(map, $("id"), $("name"), $("score"));

    Table result = table.groupBy($("name")).select($("name"), $("score").avg().as("agvScore"));
    tableEnv.toRetractStream(result,Row.class).print();

    env.execute("TableExample");
  }
}
