package com.dtstack.flink.demo.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/**
 * 用于北枫使用的
 *
 * @author beifeng
 */
public class CustomKafkaEventSchema implements DeserializationSchema<Row>, SerializationSchema<String> {

    private static final long serialVersionUID = 6154188370181669798L;


    @Override
    public byte[] serialize(String event) {
        return event.getBytes();
    }

    @Override
    public Row deserialize(byte[] message) {

        JSONObject jsonObject = JSON.parseObject(new String(message));
        return  Row.of(
                jsonObject.getString("userid"),
                jsonObject.getInteger("age"),
                jsonObject.getLong("dttime"),
                jsonObject.getInteger("cityId"),
                ""
        );
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}