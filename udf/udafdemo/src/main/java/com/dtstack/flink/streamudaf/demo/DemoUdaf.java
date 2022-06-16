package com.dtstack.flink.streamudaf.demo;

import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xiaoyu
 * @create 2021-09-18
 * udaf案例 demo
 */
public class DemoUdaf extends AggregateFunction<String, DemoUdaf.Accum> {

    private static final Logger LOG = LoggerFactory.getLogger(DemoUdaf.class);

    /**
     * 定义累加器中数据的结构
     */
    public static class Accum {
        public String middle;
    }

    /**
     * 处理完所有输入行之后调用该方法来获取最终结果
     *
     * @param accumulator
     * @return
     */
    @Override
    public String getValue(Accum accumulator) {
        return accumulator.middle;
    }

    /**
     * 创建累加器存储中间计算结果
     *
     * @return
     */
    @Override
    public Accum createAccumulator() {
        Accum accum = new Accum();
        accum.middle = "UADFdtstack";
        return accum;
    }

    /**
     * 每一次输入进行一次计算，更新累加器
     *
     * @param accum
     * @param input
     */
    public void accumulate(Accum accum, String input) {

        try {
            accum.middle += input;
        } catch (Exception e) {
            LOG.error(e.toString());
        }

    }
}
