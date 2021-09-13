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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dtstack
 * @create 2021-08-26 10:52
 * @description
 **/
public class DtStackSink extends RichSinkFunction<Person> {
    private static final Logger LOG = LoggerFactory.getLogger(DtStackSink.class);

    public transient Counter outRecords;
    public transient Counter outDirtyRecords;
    public transient Meter outRecordsRate;

    public void initMetric() {
        outRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        outDirtyRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_DIRTY_RECORDS_OUT);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(outRecords, 20));
    }

    /**
     * 打开资源比如连接
     *
     * @param parameters parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        initMetric();
    }

    @Override
    public void invoke(Person value) {
        try {
            LOG.info(value.toString());
            outRecords.inc();
        } catch (Exception e) {
            outDirtyRecords.inc();
            LOG.error(e.toString());
        }
    }

    /**
     * 释放资源比如连接
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
