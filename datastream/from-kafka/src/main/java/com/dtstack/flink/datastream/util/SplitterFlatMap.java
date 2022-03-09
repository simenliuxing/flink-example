package com.dtstack.flink.datastream.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @Author xiaoyu
 * @Create 2022/1/6 17:20
 * @Description
 */
public class SplitterFlatMap implements FlatMapFunction<Row, Tuple2<String, Integer>> {
    @Override
    public void flatMap(Row value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String name = (String) value.getField(1);
        for (String word : name.split(" ")) {
            out.collect(new Tuple2<>(word, 1));
        }
    }
}
