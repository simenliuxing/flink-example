package com.dtstack.flink.datastream.kafkaUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @Author xiaoyu
 * @Create 2022/1/6 16:53
 * @Description
 */
public class CustomKafkaSchema implements DeserializationSchema<Row>, SerializationSchema<String> {
    @Override
    public Row deserialize(byte[] message) throws IOException {
        JSONObject jsonObject = JSON.parseObject(new String(message));

        return Row.of(
                jsonObject.getInteger("id"),
                jsonObject.getString("name"),
                jsonObject.getInteger("departmentId"),
                jsonObject.getDouble("salary"),
                ""
        );
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(String element) {
        return element.getBytes();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}
