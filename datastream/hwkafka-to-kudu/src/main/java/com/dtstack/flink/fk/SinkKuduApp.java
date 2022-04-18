package com.dtstack.flink.fk;

import com.dtstack.flink.fk.func.ConvertJsonMapFunc;
import com.dtstack.flink.fk.func.ToKuduSinkFunc;
import com.dtstack.flink.fk.func.helper.KuduJdbcSinkHelper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Objects;

import static com.dtstack.flink.fk.util.GlobalConfig.*;
import static com.dtstack.flink.fk.util.KafkaConfig.getKafkaProperties;

/**
 * 主程序
 *
 * @author xuzhiwen
 */
public class SinkKuduApp {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.addSource(
            new FlinkKafkaConsumer<>(sourceTopic(), new SimpleStringSchema(), getKafkaProperties())
                .setStartFromLatest())
        .map(new ConvertJsonMapFunc())
        .filter(Objects::nonNull)
        .addSink(
            new ToKuduSinkFunc(new KuduJdbcSinkHelper<>(kuduTable(), isBatch(), pk(), isUpper())));

    env.execute("kerberos kafka to lad kudu");
  }
}
