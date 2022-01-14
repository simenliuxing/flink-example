package com.dtstack.flink.datastream;

import com.dtstack.flink.datastream.kafkaUtil.CustomKafkaSchema;
import com.dtstack.flink.datastream.util.SplitterFlatMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @Author xiaoyu
 * @Create 2022/1/6 16:37
 * @Description
 */
public class DataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.109:9092");
        properties.setProperty("group.id", "xxy");
        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer("kafka",new CustomKafkaSchema(),properties);
        DataStreamSource<Row> dataStream = env.addSource(flinkKafkaConsumer);

        //{"id":1,"name":"zhangsan","departmentId":1,"salary":10000.0}
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new SplitterFlatMap())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);
        result.print();
        env.execute("window word count");
    }

}
