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
public class UserBrowseLog {

    private String userID;

    private String eventTime;

    private String eventType;

    private String productID;

    private Integer productPrice;

}
