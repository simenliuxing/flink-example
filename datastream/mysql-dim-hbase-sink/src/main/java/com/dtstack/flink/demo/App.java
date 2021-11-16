package com.dtstack.flink.demo;

import com.dtstack.flink.demo.func.HbaseSink;
import com.dtstack.flink.demo.func.JoinMysqlDimFlatMapFunc;
import com.dtstack.flink.demo.kafka.CustomKafkaEventSchema;
import com.dtstack.flink.demo.util.GlobalConfigUtil;
import com.dtstack.flink.demo.util.KafkaUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;

/**
 * 主程序
 *
 * @author beifeng
 */
public class App {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // env.set...
        // env.getCheckpointConfig().set...

        FlinkKafkaConsumerBase<Row> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                GlobalConfigUtil.getTopic(),
                new CustomKafkaEventSchema(),
                KafkaUtil.getKafkaProperties()
        );

        flinkKafkaConsumer.setStartFromLatest();

        env.addSource(flinkKafkaConsumer)
                // 维表关联
                .flatMap(new JoinMysqlDimFlatMapFunc())
                // 一些处理
                .filter(v -> v.getField(4) != null)
                // sink hbase
                .addSink(new HbaseSink());

        env.execute("flink demo");


    }
}
