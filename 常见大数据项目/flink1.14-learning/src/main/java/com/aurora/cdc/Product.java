package com.aurora.cdc;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @desc :
 */
@Slf4j
@Data
public class Product {
    private String name;
    private String description;

    public static Product of(String json) {
        return JSON.parseObject(json, Product.class);
    }
}