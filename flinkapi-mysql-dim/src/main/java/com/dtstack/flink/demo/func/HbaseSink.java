package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.util.HbaseUtil;
import com.dtstack.flink.demo.util.MetricConstant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * sink hbase
 *
 * @author beifenng
 */
public class HbaseSink extends RichSinkFunction<Row> {
    /**
     * hbase 所需信息
     */
    private static final byte[] COLUMN_ID = HbaseUtil.toByte("id");
    private static final byte[] COLUMN_AGE = HbaseUtil.toByte("age");
    private static final byte[] COLUMN_DTTIME = HbaseUtil.toByte("dttime");
    private static final byte[] COLUMN_CITY_ID = HbaseUtil.toByte("cityId");
    private static final byte[] COLUMN_CITY_NAME = HbaseUtil.toByte("cityId");
    private static final byte[] FAMILY = HbaseUtil.toByte("info");
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSink.class);
    /**
     * metric 所需要
     */
    public transient Counter outRecords;
    public transient Counter outDirtyRecords;
    public transient Meter outRecordsRate;
    private Connection connection;
    private Table hbaseTable;

    @Override
    public void invoke(Row value, Context context) {

        Put put = new Put(HbaseUtil.toByte(value.getField(0)));
        put.addColumn(FAMILY, COLUMN_ID, HbaseUtil.toByte(value.getField(0)));
        put.addColumn(FAMILY, COLUMN_AGE, HbaseUtil.toByte(value.getField(1)));
        put.addColumn(FAMILY, COLUMN_DTTIME, HbaseUtil.toByte(value.getField(2)));
        put.addColumn(FAMILY, COLUMN_CITY_ID, HbaseUtil.toByte(value.getField(3)));
        put.addColumn(FAMILY, COLUMN_CITY_NAME, HbaseUtil.toByte(value.getField(4)));
        try {
            hbaseTable.put(put);
            outRecords.inc();
        } catch (IOException e) {
            // 这里直接抛出异常,没有捕捉
            outDirtyRecords.inc();
            LOG.error("hbase put failed !", e);
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
    public void open(Configuration parameters) {
        connection = HbaseUtil.getConnection();
        try {
            hbaseTable = connection.getTable(TableName.valueOf("xzw_test"));
        } catch (IOException e) {
            LOG.error("获取table失败 ! ", e);
        }

        outRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        outDirtyRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_DIRTY_RECORDS_OUT);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(outRecords, 20));

    }
}
