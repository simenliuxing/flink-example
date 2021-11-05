package com.dtstack.flink.demo.kafka;

/**
 * 用于kafka返回结果的,当然我这里是没有实现的
 *
 * @author beifeng
 */
public class KafkaEvent {
    // property code



    /**
     * 什么也没有做
     * @param json kafka message
     * @return kafkaEvent
     */
    public static KafkaEvent fromString(String json) {
        return new KafkaEvent();
    }
}
