package com.luoj.task.learn.hudi.example001;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;


/**
 * @author lj.michale
 * @description
 * @date 2021-06-29
 */
@Data
public class User {
    // 用户ID
    @JSONField(ordinal = 1)
    private Integer id;
    // 生日
    @JSONField(ordinal = 2, format="yyyy-MM-dd HH:mm:ss")
    private Date birthday;
    // 姓名
    @JSONField(ordinal = 3)
    private String name;
    // 创建日期
    @JSONField(ordinal = 4, format="yyyy-MM-dd HH:mm:ss")
    private Date createTime;
    // 职位
    @JSONField(ordinal = 5)
    private String position;

    public User() {
    }

    public User(Integer id, String name, Date birthday, String position) {
        this.id = id;
        this.birthday = birthday;
        this.name = name;
        this.position = position;
        this.createTime = new Date();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}