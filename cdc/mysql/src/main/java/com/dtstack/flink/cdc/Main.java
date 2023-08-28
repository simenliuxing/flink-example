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

package com.dtstack.flink.cdc;

/**
 * @author chuixue
 * @create 2023-06-29 11:14
 * @description
 **/

public class Main {
    public static String sqlText = "-- 定义配置(并行度、容错、状态后段等相关配置)，配置可以参考下面链接：\n" +
            "-- https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/config/\n" +
            "-- https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/\n" +
            "set pipeline.name = mysql-kafka;\n" +
            "set execution.checkpointing.interval = 1min;\n" +

            "-- set table.exec.resource.default-parallelism = 3;\n" +

            "set parallelism.default = 3;\n" +

            "set execution.checkpointing.checkpoints-after-tasks-finish.enabled = true;\n" +
            "set scan.incremental.close-idle-reader.enabled = true;\n" +

            "\n" +
            "-- source端配置和数据类型 参考上面\n" +
            "CREATE TABLE source\n" +
            "(\n" +
            "    id   INT,\n" +
            "    name STRING,\n" +
            "    PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "      'connector' = 'mysql-cdc',\n" +
            "      'hostname' = 'localhost',\n" +
            "      'port' = '3306',\n" +
            "      'username' = 'root',\n" +
            "      'password' = 'root',\n" +
            "      'database-name' = 'test',\n" +
            "      'table-name' = 'out_cdc');\n" +
            "\n" +
            "-- sink端配置和数据类型 参考上面\n" +
            "CREATE TABLE sink\n" +
            "(\n" +
            "    id   INT,\n" +
            "    name STRING\n" +
            ") WITH (\n" +
            "      'connector' = 'kafka',\n" +
            "      'topic' = '11111',\n" +
            "      'properties.bootstrap.servers' = '106.52.82.15:9092',\n" +
            "      'format' = 'ogg-json');\n" +
            "\n" +
            "-- 执行sql\n" +
            "insert into sink\n" +
            "select *\n" +
            "from source\n";

    public static void main(String[] args) {
        RunJob.runJobSql(sqlText);
    }
}