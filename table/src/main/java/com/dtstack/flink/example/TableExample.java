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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcUpsertTableSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * @author chuixue
 * @create 2021-09-17 15:55
 * @description
 **/
public class TableExample {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings eSeting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, eSeting);
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

        // {"id":"11","name":"ccc","age":"10"}
        // {"id":"12","name":"ccc","age":"10"}
        tableEnv
                .connect(
                        new Kafka()
                                .version("universal")
                                .topic("testIn")
                                .property("bootstrap.servers", "172.16.100.109:9092")
                                .startFromLatest()
                )
                .withFormat(
                        new Json()
                                .failOnMissingField(false)
                                .deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("id", Types.INT)
                                .field("name", Types.STRING)
                                .field("age", Types.INT)
                )
                .inAppendMode()
                .createTemporaryTable("source");


        JdbcOptions jdbcOptions = JdbcOptions.builder()
                .setDriverName("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/test?useUnicode=true&amp;characterEncoding=UTF-8")
                .setUsername("xxx")
                .setPassword("xxxx")
                .setTableName("jdbcsink")
                .build();

        TableSchema tableSchema = TableSchema.builder()
                .field("id", Types.INT)
                .field("name", Types.STRING)
                .field("age", Types.INT)
                .build();

        JdbcUpsertTableSink sink = JdbcUpsertTableSink.builder()
                .setOptions(jdbcOptions)
                .setTableSchema(tableSchema)
                .setMaxRetryTimes(3)
                .setFlushMaxSize(10)
                .setFlushIntervalMills(100)
                .build();

        // tableEnv.registerTableSink("sink", sink);
        tableEnv.sqlUpdate("insert into sink select id,name,age from source");

        Table table = tableEnv.sqlQuery("select * from source");
        tableEnv.toRetractStream(table, Row.class).print();

        env.execute("TableExample");
    }
}
