package com.dtstack.flink.streamudtf.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author longxuan
 * @create 2021-09-018 10:19
 * @description
 **/
public class UdtfDemo extends TableFunction<Tuple2<String, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(UdtfDemo.class);

    /** 定义属性，分隔符*/
    private String separator="-";


    /**
     * @param input 输入参数
     * @return 返回参数
     */
    public void eval(String input){
        try {
            for(String s:input.split(separator)){
                collect(new Tuple2<>(s,s.length()));
            }
        }catch (Exception e) {
            LOG.error(e.toString());
        }
    }
}