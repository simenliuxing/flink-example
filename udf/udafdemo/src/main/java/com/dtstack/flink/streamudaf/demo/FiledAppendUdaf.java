package com.dtstack.flink.streamudaf.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 行星 on 2021/11/03.
 * 文件内容追加的udaf
 */
public class FiledAppendUdaf extends AggregateFunction<String, Tuple2<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(FiledAppendUdaf.class);
    public static final String CHAR_1 = ",";
    public static final String CHAR_2 = "|";

    /**
     * 初始化累加器
     *
     * @return 返回累加器
     */
    @Override
    public Tuple2<String, String> createAccumulator() {
        Tuple2<String, String> acc = new Tuple2<>();
        acc.f0 = null;
        acc.f1 = null;
        return acc;
    }

    /**
     * 返回函数结果
     *
     * @param acc 累加器
     * @return 累加器的第一个值
     */
    @Override
    public String getValue(Tuple2<String, String> acc) {
        return acc.f0;
    }

    /**
     * 累加方法
     *
     * @param acc  累加器
     * @param xcyh 协储员号
     * @param rate 分成比例
     * @param dt   时间
     */
    public void accumulate(Tuple2<String, String> acc, String xcyh, String rate, String dt) {
        try {
            if (null != xcyh && null != rate && null != dt) {
                if (acc.f1 == null || !acc.f1.equals(dt)) {
                    acc.f1 = dt;
                    acc.f0 = xcyh + CHAR_2 + rate;
                } else if (acc.f1.equals(dt)) {
                    if (!acc.f0.contains(xcyh)) {
                        acc.f0 += CHAR_1 + xcyh + CHAR_2 + rate;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.toString(), e);
        }
    }
}
