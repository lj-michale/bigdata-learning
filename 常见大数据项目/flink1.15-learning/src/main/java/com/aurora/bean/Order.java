package com.aurora.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lj.michale
 * @description
 * @date 2022-04-01
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private String id;
    private Integer userId;
    private Integer money;
    private Long createTime;

}
