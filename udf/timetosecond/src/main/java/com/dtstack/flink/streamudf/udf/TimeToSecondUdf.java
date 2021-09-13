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

package com.dtstack.flink.streamudf.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: chuixue
 * @create: 2019-09-24 13:46
 * @description: time to second udf
 **/
public class TimeToSecondUdf extends ScalarFunction {
    public static Map<String, DateTimeFormatter> dateTimeFormatterMap
            = new ConcurrentHashMap<String, DateTimeFormatter>();

    public long eval(String timestamp, String format) {
        DateTimeFormatter ftf = null;
        if (dateTimeFormatterMap.containsKey(format)) {
            ftf = dateTimeFormatterMap.get(format);
        } else {
            ftf = DateTimeFormatter.ofPattern(format);
            dateTimeFormatterMap.put(format, ftf);
        }
        LocalDateTime parse = LocalDateTime.parse(timestamp, ftf);
        return LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toEpochSecond();
    }
}

