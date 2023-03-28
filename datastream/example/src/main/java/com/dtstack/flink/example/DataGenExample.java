/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.example;

import com.dtstack.flink.example.bean.Person;
import com.dtstack.flink.example.function.DtStackMapperWithMetrics;
import com.dtstack.flink.example.function.DtStackSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author dtstack
 * @create 2021-08-25 17:17
 * @description todo
 * 1.flink任务的ha已经在数栈的控制台配置好,工程中无需另外操作
 * 2.Prometheus已经在数栈的控制台配置好,工程中无需另外操作
 * 3.如果使用附加资源比如keytab、krb5文件,只需要在任务管理选中该任务,然后在附加资源选择即可,然后在代码中使用到资源的地方只需要使用./keytab、./krb5路径即可
 **/
public class DataGenExample {
    public static void main(String[] args) throws Exception {
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

        // 解析source端数据
        // {"name":"dtstack","age":"12","timestamp":"2021-08-08 10:10:10"}
        // {"name":"dtstack","age":"12","timestamp":"2021-08-08 10:10:20"}
        SingleOutputStreamOperator<Person> map = env.addSource(new RichSourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws InterruptedException {
                while (true) {
                    ctx.collect("{\"name\":\"dtstack\",\"age\":\"12\",\"timestamp\":\"2021-08-08 10:10:20\"}");
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        }).map(new DtStackMapperWithMetrics());

        // 输出结果
        map.addSink(new DtStackSink());

        env.execute("DataStreamExample");
    }
}
