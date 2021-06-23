package com.luoj.task.learn.join.example001;

import lombok.Data;

import java.io.Serializable;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-23
 */
@Data
public class CityInfo implements Serializable {
    private Integer cityId;
    private String cityName;
    private Long ts;

}