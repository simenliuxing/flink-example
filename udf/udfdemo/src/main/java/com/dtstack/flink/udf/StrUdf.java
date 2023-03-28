package com.dtstack.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author xiaoyu
 * @Create 2022/1/6 19:14
 * @Description
 */
public class StrUdf extends ScalarFunction {

    public String eval(String name) {
        return name + "__dtstack";
    }
}
