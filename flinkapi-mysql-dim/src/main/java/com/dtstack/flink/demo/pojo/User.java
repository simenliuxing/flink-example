package com.dtstack.flink.demo.pojo;

/**
 * 一个bean对象用于测试维表关联的结果
 *
 * @author beifeng
 */
public class User {
    String userid;
    int age;
    long dttime;
    String orderId;
    String orderTime;

    public User(String userid, int age, long dttime, String orderId, String orderTime) {
        this.userid = userid;
        this.age = age;
        this.dttime = dttime;
        this.orderId = orderId;
        this.orderTime = orderTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    @Override
    public String toString() {
        return "User{" +
                "userid='" + userid + '\'' +
                ", age=" + age +
                ", dttime=" + dttime +
                ", orderId='" + orderId + '\'' +
                ", orderTime='" + orderTime + '\'' +
                '}';
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public long getDttime() {
        return dttime;
    }

    public void setDttime(long dttime) {
        this.dttime = dttime;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }
}
