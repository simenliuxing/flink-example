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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author chuixue
 * @create 2022-06-16 14:14
 * @description : Caffeine缓存
 **/
public class CaffeineUdf extends ScalarFunction {
    private final Logger LOG = LoggerFactory.getLogger(CaffeineUdf.class);

    private transient Cache<String, String> cache;
    /**
     * 缓存条数
     */
    private Long cacheSize = 1000000L;
    /**
     * 超时时间,单位小时
     */
    private Long timeOut = 1L;

    @Override
    public void open(FunctionContext context) throws Exception {
        initCache();
        // 初始化数据库链接
    }


    public String eval(String key) {
        // 缓存数据是否存在
        String value = cache.getIfPresent(key);
        if (value != null) {
            return value;
        } else {
            // 从数据库中获取
            String dbValue = "";
            // 并将数据放入缓存
            cache.put(key, dbValue);
            // 将数据获取的值返回
            value = dbValue;
        }
        return value;
    }

    @Override
    public void close() throws Exception {
        // 关闭数据库链接
    }

    private void initCache() {
        cache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(timeOut, TimeUnit.HOURS)
                .build();
    }
}
