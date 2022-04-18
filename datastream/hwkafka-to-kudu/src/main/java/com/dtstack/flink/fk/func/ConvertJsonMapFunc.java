package com.dtstack.flink.fk.func;

import com.dtstack.flink.fk.pojo.KafkaRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author beifeng */
public class ConvertJsonMapFunc implements MapFunction<String, KafkaRecord> {
  private static final Gson GSON = new GsonBuilder().create();
  private static final Logger LOG = LoggerFactory.getLogger(ConvertJsonMapFunc.class);
  private int dirtyLimit = 100;
  private int dirtyCount = 0;

  public ConvertJsonMapFunc() {}

  public ConvertJsonMapFunc(int dirtyLimit) {
    this.dirtyLimit = dirtyLimit;
  }

  @Override
  public KafkaRecord map(String json) {
    if (StringUtils.isBlank(json)) {
      LOG.warn("A null json was received");
      dirty0(++dirtyCount);
      return null;
    }

    try {
      // 简单的json的解析
      return GSON.fromJson(json, KafkaRecord.class);
    } catch (JsonSyntaxException e) {
      dirty0(++dirtyCount);
      LOG.warn("Json parsing failure!  json:{} , e : {}", json, e);
    }

    return null;
  }

  public void dirty0(int count) {
    if (count >= dirtyLimit) {
      throw new RuntimeException(
          String.format("The dirty data exceeded the limited %s ", dirtyLimit));
    }
  }
}
