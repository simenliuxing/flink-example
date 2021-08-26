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

package com.dtstack.flink.example.function;

import com.dtstack.flink.example.bean.Person;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dtstack
 * @create 2021-08-26 10:45
 * @description
 **/
public class DtStackWaterMark extends BoundedOutOfOrdernessTimestampExtractor<Person> {
    private static final Logger LOG = LoggerFactory.getLogger(DtStackWaterMark.class);

    private Long currentMaxTimestamp = 0L;

    public DtStackWaterMark(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Person element) {
        try {
            currentMaxTimestamp = element.getTimestamp().getTime();
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return currentMaxTimestamp;
    }
}
