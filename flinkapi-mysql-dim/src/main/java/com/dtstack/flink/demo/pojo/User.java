package com.dtstack.flink.demo.pojo;

/**
 * 一个bean对象用于测试维表关联的结果
 *
 * @author beifeng
 */
public class User {
   private  String userid;
   private  int age;
   private  long dttime;
   private  int cityId;
    private String cityName;

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
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

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public User(String userid, int age, long dttime, int cityId, String cityName) {
        this.userid = userid;
        this.age = age;
        this.dttime = dttime;
        this.cityId = cityId;
        this.cityName = cityName;
    }
}
