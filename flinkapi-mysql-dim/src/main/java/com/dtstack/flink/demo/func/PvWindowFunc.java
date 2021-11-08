package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.pojo.PvUvInfo;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 获取聚合结果发送下游
 *
 * @author beifeng
 */
public class PvWindowFunc implements WindowFunction<PvUvInfo, PvUvInfo, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<PvUvInfo> input, Collector<PvUvInfo> out) throws Exception {
        PvUvInfo pvUvInfo = input.iterator().next();
        pvUvInfo.setUserId(key);
        pvUvInfo.setStartAndEndTime(window);
        out.collect(pvUvInfo);

    }
}
