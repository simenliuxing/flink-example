package com.dtstack.flink.demo.func;

import com.dtstack.flink.demo.pojo.PvUvInfo;
import com.dtstack.flink.demo.pojo.User;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 统计key的pv
 *
 * @author beifeng
 */
public class PvAggFunc implements AggregateFunction<User, PvAggFunc.Acc, PvUvInfo> {

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(User user, Acc acc) {
        return acc.incr();
    }

    @Override
    public PvUvInfo getResult(Acc acc) {
        return acc.getValue();
    }

    @Override
    public Acc merge(Acc acc, Acc acc1) {
        throw new RuntimeException("禁止合并");
    }

    /**
     * 用于统计pv uv的累加器
     */
    public static class Acc {
        private final PvUvInfo result = new PvUvInfo();

        public Acc incr() {
            result.getPv().incrementAndGet();
            return this;
        }

        public PvUvInfo getValue() {
            return result;
        }

    }
}
