package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.pojo.User;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * 剔除维表关联失败的数据
 *
 * @author beifeng
 */
public class EvictorFunc implements Evictor<User, TimeWindow> {
    @Override
    public void evictBefore(Iterable<TimestampedValue<User>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        Iterator<TimestampedValue<User>> iterator = elements.iterator();
        while (iterator.hasNext()) {
            String cityName = iterator.next().getValue().getCityName();
            // 没有关联上的维表剔除掉
            if (StringUtils.isBlank(cityName)) {
                iterator.remove();
            }
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<User>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        // NOTHING
    }
}
