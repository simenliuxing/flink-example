package com.dtstack.flink.udaf;

import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author xiaoyu
 * @Create 2022/1/6 19:42
 * @Description 使用场景，求平均销售额。
 */
public class UdafDemo extends AggregateFunction<Double, UdafDemo.WeightedAvgAccumulator> {

    private static final Logger LOG = LoggerFactory.getLogger(UdafDemo.class);

    //定义累加器中的数据结构
    public static class WeightedAvgAccumulator {
        public Double sum = 0.0;
        public Integer count = 0;
    }

    @Override
    public WeightedAvgAccumulator createAccumulator() {
        return new WeightedAvgAccumulator();
    }

    //返回最终结果
    @Override
    public Double getValue(WeightedAvgAccumulator accumulator) {
        if (accumulator.count == 0) {
            return null;
        } else {
            return accumulator.sum / accumulator.count;
        }
    }

    //中间的累加过程
    public void accumulate(WeightedAvgAccumulator acc, Double iValue, Integer iWeight) {
        try {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    //发生错误的话 可以回撤
    public void retract(WeightedAvgAccumulator acc, Double iValue, Integer iWeight) {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }

    //将每一次迭代的数据累加
    public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> iterable) {
        for (WeightedAvgAccumulator weightedAvgAccumulator : iterable) {
            acc.count += weightedAvgAccumulator.count;
            acc.sum += weightedAvgAccumulator.sum;
        }
    }

    //重制累加器
    public void resetAccumulator(WeightedAvgAccumulator acc) {
        acc.count = 0;
        acc.sum = 0.0;
    }

}


