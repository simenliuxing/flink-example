package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.pojo.PvUvInfo;
import com.dtstack.flink.demo.util.HbaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * sink hbase
 *
 * @author beifenng
 */
public class HbaseSink extends RichSinkFunction<PvUvInfo> {
    /**
     * hbase 所需信息
     */
    private static final byte[] COLUMN_PV = Bytes.toBytes("pv");
    private static final byte[] COLUMN_UV = Bytes.toBytes("uv");
    private static final byte[] COLUMN_START_TIME = Bytes.toBytes("start_time");
    private static final byte[] COLUMN_END_TIME = Bytes.toBytes("end_time");
    private static final byte[] FAMILY = Bytes.toBytes("data");
    private Connection connection;
    private Table hbaseTable;

    @Override
    public void invoke(PvUvInfo value, Context context) {

        Put put = new Put(Bytes.toBytes(value.getStartTime()));
        put.addColumn(FAMILY, COLUMN_PV, Bytes.toBytes(value.getPv().get()));
        put.addColumn(FAMILY, COLUMN_UV, Bytes.toBytes(value.getUv().get()));
        put.addColumn(FAMILY, COLUMN_END_TIME, Bytes.toBytes(value.getEndTime()));
        put.addColumn(FAMILY, COLUMN_START_TIME, Bytes.toBytes(value.getStartTime()));
        try {
            hbaseTable.put(put);
        } catch (IOException e) {
            throw new RuntimeException("插入hbase失败");
        }

    }

    @Override
    public void close() throws Exception {
        if (hbaseTable != null) {
            hbaseTable.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HbaseUtil.getConnection();
        hbaseTable = connection.getTable(TableName.valueOf("xzw_test"));
    }
}
