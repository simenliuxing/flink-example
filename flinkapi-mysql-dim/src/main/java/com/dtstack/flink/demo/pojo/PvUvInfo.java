package com.dtstack.flink.demo.pojo;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 用于封装 pv uv的实体
 *
 * @author beifeng
 */
public class PvUvInfo {
    private AtomicLong pv = new AtomicLong(0);
    private AtomicLong uv = new AtomicLong(0);
    private String userId;
    private long startTime;
    private long endTime;

    @Override
    public String toString() {
        return "PvUvInfo{" +
                "pv=" + pv +
                ", uv=" + uv +
                ", userId='" + userId + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public AtomicLong getUv() {
        return uv;
    }

    public void setUv(AtomicLong uv) {
        this.uv = uv;
    }

    public AtomicLong getPv() {
        return pv;
    }

    public void setPv(AtomicLong pv) {
        this.pv = pv;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setStartAndEndTime(TimeWindow window) {
        startTime = window.getStart();
        endTime = window.getEnd();
    }

    public void merge(PvUvInfo value) {
        pv.addAndGet(value.getPv().get());
        uv.addAndGet(value.getUv().get());
    }

    public void acceptPv(long longValue) {
        pv.addAndGet(longValue);
    }
}
