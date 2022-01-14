package com.dtstack.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author xiaoyu
 * @Create 2022/1/6 19:14
 * @Description
 */
public class UdfDemo extends ScalarFunction {

    private final Logger LOG = LoggerFactory.getLogger(UdfDemo.class);

    public String eval(String name) {
        String finalName = "";
        try {

            if (!"".equals(name) && name != null) {
                finalName = name + "dtstack";
            } else {
                throw new Exception("The field you entered does not meet requirements");
            }

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.toString());
        }
        return finalName;
    }
}
