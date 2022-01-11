package com.dtstack.flink;

import com.dtstack.flink.task.MysqlLoopSourceFunc;
import com.dtstack.flink.task.RemoteInvokeSinkFunc;
import com.dtstack.flink.util.GlobalConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author beifeng
 */
public class DDLChangeApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 测试用,测完需要删除
        env.setStateBackend(new MemoryStateBackend());
        env.getCheckpointConfig().setCheckpointInterval(GlobalConfig.getCheckpointInterval());
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.milliseconds(1000), Time.milliseconds(1000)));

        env
                .addSource(new MysqlLoopSourceFunc())
                .addSink(new RemoteInvokeSinkFunc(GlobalConfig.getRemoteIp(), GlobalConfig.getRemotePort()));


        env.execute("ddl change");


    }
}
