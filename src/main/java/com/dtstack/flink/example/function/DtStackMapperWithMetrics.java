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

import com.dtstack.flink.example.constant.MetricConstant;
import com.dtstack.flink.example.bean.Person;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * @author dtstack
 * @create 2021-08-26 09:41
 * @description
 **/
public class DtStackMapperWithMetrics extends RichMapFunction<String, Person> {

    private static final Logger LOG = LoggerFactory.getLogger(DtStackMapperWithMetrics.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    /**
     * tps ransactions Per Second
     */
    protected transient Counter numInRecord;

    protected transient Meter numInRate;

    /**
     * rps Record Per Second: deserialize data and out record num
     */
    protected transient Counter numInResolveRecord;

    protected transient Meter numInResolveRate;

    protected transient Counter numInBytes;

    protected transient Meter numInBytesRate;

    protected transient Counter dirtyDataCounter;


    public void initMetric() {
        dirtyDataCounter = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_DIRTY_DATA_COUNTER);

        numInRecord = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_IN_COUNTER);
        numInRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_IN_RATE, new MeterView(numInRecord, 20));

        numInBytes = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_BYTES_IN_COUNTER);
        numInBytesRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_BYTES_IN_RATE, new MeterView(numInBytes, 20));

        numInResolveRecord = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_COUNTER);
        numInResolveRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_RATE, new MeterView(numInResolveRecord, 20));
    }

    @Override
    public void open(Configuration parameters) {
        initMetric();
    }

    @Override
    public Person map(String value) {
        try {
            Person p = new Person();
            numInRecord.inc();
            numInBytes.inc(value.getBytes().length);

            JsonNode jsonNode = objectMapper.readTree(value);
            p.setName(jsonNode.get("name").asText());
            p.setAge(jsonNode.get("age").asInt());
            p.setTimestamp(Timestamp.valueOf(jsonNode.get("timestamp").asText()));

            numInResolveRecord.inc();
            return p;
        } catch (Exception e) {
            dirtyDataCounter.inc();
            LOG.error(e.toString());
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
