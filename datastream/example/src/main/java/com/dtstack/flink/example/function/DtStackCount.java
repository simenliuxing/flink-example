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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author dtstack
 * @create 2021-08-26 11:43
 * @description
 **/
public class DtStackCount extends RichFlatMapFunction<Person, Tuple2<String, Integer>> {

    private transient ValueState<Tuple2<String, Integer>> sum;

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ValueStateDescriptor(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }),
                        Tuple2.of("", 0));
        sum = getRuntimeContext().getState(descriptor);

        // 设置ttl
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        descriptor.enableTimeToLive(ttlConfig);
    }

    @Override
    public void flatMap(Person value, Collector<Tuple2<String, Integer>> out) throws Exception {
        // access the state value
        Tuple2<String, Integer> currentSum = sum.value();

        // update the count
        currentSum.f0 = value.getName();
        currentSum.f1 = currentSum.f1 + value.getAge();

        // update the state
        sum.update(currentSum);

        out.collect(new Tuple2<>(value.getName(), currentSum.f1));
    }

    @Override
    public void close() {

    }
}
