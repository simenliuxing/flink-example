package com.dtstack.flink.task;

import com.dtstack.flink.util.MysqlUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * 循环查询mysql获取最新ddl结果发送下游
 *
 * @author beifeng
 */
public class MysqlLoopSourceFunc extends RichSourceFunction<Row> implements CheckpointedFunction {

    private static final Logger log = LoggerFactory.getLogger(MysqlLoopSourceFunc.class);
    private BigDecimal cur = new BigDecimal("0.0");
    private ListState<BigDecimal> breakpoint;
    private Connection connection;
    private boolean running = true;
    private PreparedStatement ps;

    @Override
    public void run(SourceContext<Row> ctx) {
        try {
            Row row;
            ResultSet rs = null;
            String ts = null;
            while (running) {
                try {

                    ps.setBigDecimal(1, cur);
                    rs = ps.executeQuery();

                    while (rs.next()) {
                        ts = rs.getString("ts");
                        row = Row.of(rs.getString("lsn"));
                        ctx.collect(row);
                    }

                    if (ts != null) {
                        this.cur = new BigDecimal(ts);
                    }

                } catch (SQLException e) {
                    log.error("query failed, cause:", e);
                } finally {
                    closeResultSet(rs);
                }

                // 休息5毫秒
                try {
                    TimeUnit.MILLISECONDS.sleep(5);
                } catch (InterruptedException ignored) {
                    // HE IS NOW NOTHING
                }

            }

        } finally {
            running = false;
        }
    }

    private void closeResultSet(ResultSet rs) {
        try {
            if (rs != null && !rs.isClosed()) {
                rs.close();
            }
        } catch (SQLException e) {
            log.warn("rs closing error, e:{}", e.getMessage());
        }
    }

    @Override
    public void cancel() {
        if (running) {
            running = false;
        }
    }

    @Override
    public void close() throws Exception {
        MysqlUtil.close(connection);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MysqlUtil.getConnection();
        ps = connection.prepareStatement(
                "select unix_timestamp(update_time) as ts,lsn from sync_info.ddl_change where unix_timestamp(update_time)>? order by update_time"
        );
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        BigDecimal value = this.cur;
        breakpoint.clear();
        breakpoint.add(value);
        if (log.isDebugEnabled()) {
            log.debug("The snapshot is triggered to save the state, state:{}", value);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        breakpoint = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("breakpoint", BigDecimal.class));

        if (context.isRestored()) {
            final Iterator<BigDecimal> iterator = breakpoint.get().iterator();
            iterator.forEachRemaining(bigDecimal -> {
                if (cur.compareTo(bigDecimal) <= 0) {
                    cur = bigDecimal;

                }
            });
            if (log.isDebugEnabled()) {
                log.debug("After the state is recovered,state :{}", cur);
            }
        }

        if (!running) {
            running = true;
        }
    }
}
