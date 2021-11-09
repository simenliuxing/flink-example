package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.pojo.User;
import com.dtstack.flink.demo.util.MetricConstant;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

import java.sql.*;

/**
 * 简单的维表关联
 *
 * @author beifeng
 */
public class JoinMysqlDimMapFunc extends RichMapFunction<User, User> {
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

    public static void main(String[] args) throws SQLException {
        Connection connection1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println(connection1);
        Connection connection2 = DriverManager.getConnection("jdbc:mysql://192.168.107.237:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println(connection2);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://192.168.107.237:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
                , "root", "root123456");
        System.out.println("mysql 连接成功");

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
    public User map(User user) {
        try {
            numInRecord.inc();
            numInBytes.inc(user.toString().getBytes().length);
            numInResolveRecord.inc();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(
                    "select * from mysqldim where id =" + user.getCityId() + " ;"
            );
            if (rs.next()) {
                user.setCityName(rs.getString("city_name"));
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            dirtyDataCounter.inc();
            e.printStackTrace();
        }
        return user;
    }

}
