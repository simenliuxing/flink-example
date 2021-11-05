package com.dtstack.flink.demo.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * 用于北枫使用的
 *
 * @param <OUT> 输出类型
 * @author beifeng
 */
public class CustomKafkaEventSchema<OUT> implements DeserializationSchema<OUT>, SerializationSchema<String> {

    private static final long serialVersionUID = 6154188370181669798L;

    private final Class<OUT> clazz;

    public CustomKafkaEventSchema(Class<OUT> clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte[] serialize(String event) {
        return event.getBytes();
    }

    @Override
    public OUT deserialize(byte[] message) throws IOException {
        return JSON.parseObject(new String(message), clazz);
    }

    @Override
    public boolean isEndOfStream(OUT nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return TypeInformation.of(clazz);
    }
}