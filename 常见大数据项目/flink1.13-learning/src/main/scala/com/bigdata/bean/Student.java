package com.bigdata.bean;

import lombok.Data;
import java.util.Date;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-13
 */
@Data
public class Student {

    private int id;

    private String name;

    private int age;

    private String address;

    private Date checkInTime;

    private long successTimeStamp;

    public Student() {
    }

    public Student(int id, String name, int age, String address) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.address = address;
    }

    public static Student of(int id, String name, int age, String address, long timeStamp) {
        Student student = new Student(id, name, age, address);
        student.setSuccessTimeStamp(timeStamp);
        return student;
    }
}