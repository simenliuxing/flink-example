package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.pojo.PvUvInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 统计 pv uv 信息
 *
 * @author beifeng
 */
public class PvUvKeyedProcessFunc extends KeyedProcessFunction<Long, PvUvInfo, PvUvInfo> {

    private ValueState<PvUvInfo> pvUvState;

    @Override
    public void open(Configuration parameters) {
        pvUvState = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("pvuv", PvUvInfo.class));
    }

    @Override
    public void processElement(PvUvInfo value,
                               KeyedProcessFunction<Long, PvUvInfo, PvUvInfo>.Context ctx,
                               Collector<PvUvInfo> out) throws Exception {
        PvUvInfo pvUvInfo = pvUvState.value();

        if (pvUvInfo == null) {
            pvUvInfo = value;
        } else {
            pvUvInfo.acceptPv(value.getPv().longValue());
        }

        pvUvInfo.getUv().incrementAndGet();
        pvUvState.update(pvUvInfo);

        out.collect(pvUvInfo);
    }
}
