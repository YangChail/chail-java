package com.chail.flink.model;

import java.sql.Timestamp;

/**
 * @author : yangc
 * @date :2023/6/30 9:57
 * @description :
 * @modyified By:
 */
public class Event {
    private String user;
    private String url;

    private Long time;

    private Integer age;


    public Event() {

    }


    public Event(String user, String url, Long time,Integer age) {
        this.user = user;
        this.url = url;
        this.time = time;
        this.age=age;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }


    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", time=" + new Timestamp(time) +
                ", age=" + age +
                '}';
    }
}
