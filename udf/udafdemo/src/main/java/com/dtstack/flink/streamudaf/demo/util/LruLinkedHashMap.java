package com.dtstack.flink.streamudaf.demo.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LruLinkedHashMap<String, Object> extends LinkedHashMap<String, Object> {
    private static final long serialVersionUID = 1L;
    private final int initialCapacity;

    //通过LinkedHashMap构造函数中的参数accessOrder来指定数据存储的顺序（false为插入顺序，true为访问顺序）
    public LruLinkedHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor, true);
        this.initialCapacity = initialCapacity;
    }

    //每次添加数据的时候，就会自动判断是否个数已经超过maximumSize，如果超过就删掉最旧的那条（相当于是FIFO算法）。
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
        return size() > this.initialCapacity;
    }
}
