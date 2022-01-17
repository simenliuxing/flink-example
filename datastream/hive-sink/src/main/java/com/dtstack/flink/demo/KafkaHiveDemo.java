package com.dtstack.flink.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.dtstack.flink.function.HiveSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author xiaoyu
 * @Create 2021/11/9 17:02
 * @Description
 */
public class KafkaHiveDemo {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // todo flink cp只需要配置这些即可，其他已经在数栈的控制台配置好(RocksDB、增量cp),工程中无需另外操作
        // 设置检查点时间周期5分钟，设置该参数但表开启cp
        env.enableCheckpointing(300000);
        // 设置两个检查点时间间隔1分钟
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000);
        // 设置检查点超时时间10分钟
        env.getCheckpointConfig().setCheckpointTimeout(600000);

        // 6分钟内可以重启3次 每次重启都间隔10秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(6, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));

        //配置kafka数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","ip:port");
        properties.setProperty("group.id","dtstack");

        //{"id":1,"name":"zhnag","age":11}
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("kafka",new SimpleStringSchema(),properties);
        DataStreamSource<String> dataStreamSource = env.addSource(flinkKafkaConsumer);


        String[] fieldNames = {"id", "name", "age"};
        TypeInformation[] types = {Types.INT, Types.STRING, Types.INT};

        SingleOutputStreamOperator<Row> mapedStream= dataStreamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                Integer id = jsonObject.getInteger("id");
                String name = jsonObject.getString("name");
                Integer age = jsonObject.getInteger("age");

                return Row.of(id,name,age);
            }
        }, new RowTypeInfo(types, fieldNames));

        mapedStream.addSink(new HiveSink());
        env.execute("kafka->hive");

    }
}
