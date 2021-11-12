package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.util.MetricConstant;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * 简单的维表关联
 *
 * @author beifeng
 */
public class JoinMysqlDimFlatMapFunc extends RichFlatMapFunction<Row, Row> {
    private final static Logger LOG = LoggerFactory.getLogger(JoinMysqlDimFlatMapFunc.class);
    /**
     * tps ransactions Per Second
     */
    protected transient Counter numInRecord;
    protected transient Meter numInRate;
    /**
     * rps Record Per Second: deserialize data and out record num
     */
    protected transient Counter numInResolveRecord;
    protected transient Meter numInResolveRate;
    protected transient Counter numInBytes;
    protected transient Meter numInBytesRate;
    protected transient Counter dirtyDataCounter;
    Connection connection;

    @Override
    public void open(Configuration parameters) {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://192.168.107.237:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                    , "root", "root123456");
            LOG.info("mysql connect succeed !");
        } catch (SQLException e) {
            LOG.error("mysql connect failed !", e);
        }
        dirtyDataCounter = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_DIRTY_DATA_COUNTER);
        numInRecord = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_IN_COUNTER);
        numInRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_IN_RATE, new MeterView(numInRecord, 20));
        numInBytes = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_BYTES_IN_COUNTER);
        numInBytesRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_BYTES_IN_RATE, new MeterView(numInBytes, 20));
        numInResolveRecord = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_COUNTER);
        numInResolveRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_RATE, new MeterView(numInResolveRecord, 20));
    }

    @Override
    public void close() throws Exception {
        if (!connection.isClosed()) {
            connection.close();
        }
    }


    @Override
    public void flatMap(Row row, Collector<Row> collector) throws Exception {
        Object cityId = row.getField(3);
        String sql = "select * from mysqldim where id =" + cityId + " ;";
        if (LOG.isDebugEnabled()) {
            LOG.debug("sql : {}", sql);
        }
        try {
            numInRecord.inc();
            numInBytes.inc(row.toString().getBytes().length);
            numInResolveRecord.inc();

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                row.setField(4, rs.getString("city_name"));
                collector.collect(row);
            }

            rs.close();
            statement.close();
        } catch (SQLException e) {
            dirtyDataCounter.inc();
            LOG.error("side join fails ,input : {} , sql :  {}, /n  cause : {}", row, sql, e);
        }

    }
}
