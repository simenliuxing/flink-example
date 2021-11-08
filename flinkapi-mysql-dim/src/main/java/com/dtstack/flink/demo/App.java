package com.dtstack.flink.demo;

import com.dtstack.flink.demo.func.*;
import com.dtstack.flink.demo.kafka.CustomKafkaEventSchema;
import com.dtstack.flink.demo.pojo.PvUvInfo;
import com.dtstack.flink.demo.pojo.User;
import com.dtstack.flink.demo.util.GlobalConfigUtil;
import com.dtstack.flink.demo.util.KafkaUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

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

        FlinkKafkaConsumerBase<User> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                GlobalConfigUtil.getTopic(),
                new CustomKafkaEventSchema<>(User.class),
                KafkaUtil.getKafkaProperties()
        );

        flinkKafkaConsumer.setStartFromLatest()
                .assignTimestampsAndWatermarks(new CustomWatermark());

        env.addSource(flinkKafkaConsumer)
                // 维表关联
                .map(new JoinMysqlDimMapFunc())
                // 一些处理
                .keyBy(User::getUserid)
                .timeWindow(Time.seconds(5), Time.seconds(5))
                .evictor(new EvictorFunc())
                .aggregate(
                        new PvAggFunc(),
                        new PvWindowFunc()
                )
                .keyBy(PvUvInfo::getEndTime)
                .process(new PvUvKeyedProcessFunc())
                // sink hbase
                .addSink(new HbaseSink());

        env.execute("flink demo");


    }


    static class CustomWatermark implements AssignerWithPeriodicWatermarks<User> {
        private static final long serialVersionUID = -742759155861320823L;
        private long currentTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(User event, long previousElementTimestamp) {
            this.currentTimestamp = Math.max(event.getDttime(), currentTimestamp);
            return event.getDttime();
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}
