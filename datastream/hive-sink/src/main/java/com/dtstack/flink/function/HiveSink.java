package com.dtstack.flink.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 批量写入的参数
 */


/**
 * @Author xiaoyu
 * @Create 2021/11/11 18:46
 * @Description
 */
public class HiveSink extends RichSinkFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(HiveSink.class);
    private Connection connection = null;
    private Statement pstmt = null;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;

    private transient List<Row> rows;

    @Override
    public void open(Configuration parameters) {
        rows = new ArrayList<>();
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://ip:port", "******", "******");
            pstmt = connection.createStatement();

            LOG.info("connection = " + connection);

            this.scheduler = new ScheduledThreadPoolExecutor(1);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(
                    () -> {
                        try {
                            synchronized (HiveSink.this) {
                                if (rows.size() > 0) {
                                    flush();
                                }
                            }
                        } catch (Exception e) {
                            LOG.error("ScheduledTask add record error", e);
                        }
                    }, 3, 3, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("error", e);
        }
    }

    @Override
    public synchronized void invoke(Row value, Context context) {
        rows.add(value);
        if (rows.size() > 100) {
            flush();
        }
    }

    public synchronized void flush() {
        try {
            StringBuffer sql = new StringBuffer("insert into bigdatadev_kafka_hive values");
            for (Row row : rows) {
                sql.append("(");
                Integer id = (Integer) row.getField(0);
                String name = (String) row.getField(1);
                Integer age = (Integer) row.getField(2);
                sql.append(id+", \""+name+"\","+age);
                sql.append(")");
            }
            pstmt.execute(sql.toString());
        } catch (Exception e) {
            LOG.error(e.toString());
        } finally {
            rows.clear();
        }
    }

    @Override
    public void close() throws Exception {
        if (pstmt != null) {
            pstmt.close();
        }
        if (connection != null) {
            connection.close();
        }
        if (scheduler == null) {
            scheduler.shutdown();
        }
    }
}
