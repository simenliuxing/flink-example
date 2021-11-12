package com.dtstack.flink.demo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * 获取hbase连接的工具类
 *
 * @author beifeng
 */
public class HbaseUtil {
    private static final Connection CONNECTION;

    static {
        try {
            Configuration configuration = HBaseConfiguration.create();

            // configuration.set("hbase.zookeeper.property.dataDir","/hbase2");
            // configuration.set("zookeeper.znode.parent","/hbase2");
            // configuration.set("hbase.unsafe.stream.capability.enforce","false");

            CONNECTION = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException("hbase 连接失败");
        }
    }

    public static Connection getConnection() {
        return CONNECTION;
    }


    public static byte[] toByte(Object value) {
        if (Objects.isNull(value)) {
            return new byte[]{};
        }
        if (value instanceof Integer) {
            return Bytes.toBytes((Integer) value);
        } else if (value instanceof Boolean) {
            return Bytes.toBytes((Boolean) value);
        } else if (value instanceof ByteBuffer) {
            return Bytes.toBytes((ByteBuffer) value);
        } else if (value instanceof Double) {
            return Bytes.toBytes((Double) value);
        } else if (value instanceof Float) {
            return Bytes.toBytes((Float) value);
        } else if (value instanceof Long) {
            return Bytes.toBytes((Long) value);
        } else if (value instanceof Short) {
            return Bytes.toBytes((Short) value);
        } else if (value instanceof String) {
            return Bytes.toBytes(String.valueOf(value));
        } else if (value instanceof BigDecimal) {
            return Bytes.toBytes((BigDecimal) value);
        }
        throw new RuntimeException("unkown dateType[" + value.toString() + "]");
    }


}
