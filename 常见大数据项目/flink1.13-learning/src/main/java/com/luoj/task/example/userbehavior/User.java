package com.luoj.task.example.userbehavior;

import lombok.Data;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
@Data
public class User {
    /**
     * userId
     */
    private Long userId;
    /**
     * 注册时间
     */
    private Long registerTime;
    /**
     * 上次登录时间
     */
    private Long lastLoadTime;
}

