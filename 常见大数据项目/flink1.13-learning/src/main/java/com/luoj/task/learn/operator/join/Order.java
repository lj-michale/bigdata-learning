package com.luoj.task.learn.operator.join;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-26
 */

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Order {

    public String userId;
    public String price;
    public Long timestamp;
    public String orderId;
    
}
