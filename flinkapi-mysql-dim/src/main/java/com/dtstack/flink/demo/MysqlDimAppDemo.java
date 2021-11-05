package com.dtstack.flink.demo;

import com.dtstack.flink.demo.func.MapFunc;
import com.dtstack.flink.demo.kafka.CustomKafkaEventSchema;
import com.dtstack.flink.demo.pojo.User;
import com.dtstack.flink.demo.util.GlobalConfigUtil;
import com.dtstack.flink.demo.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

/**
 * 主程序
 *
 * @author beifeng
 */
public class MysqlDimAppDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
               .map(new MapFunc())
                .print();


        env.execute(" join mysql dim demo.");


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
