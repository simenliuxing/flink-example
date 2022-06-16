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

package com.dtstack.flink.streamudf.demo;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chuixue
 * @create 2021-09-08 14:19
 * @description udf案例
 **/
public class DemoUdf extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DemoUdf.class);

    /**
     * @param input 输入参数
     * @return 返回参数
     */
    public String eval(String input) {
        String output = "";
        try {
            output = input + " dtstack-----";
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return output;
    }
}
