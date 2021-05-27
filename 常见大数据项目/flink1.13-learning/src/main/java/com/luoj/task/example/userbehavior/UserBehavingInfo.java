package com.luoj.task.example.userbehavior;

import lombok.*;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class UserBehavingInfo {

    private String userNo;

    /**
     * 用户行为
     */
    private String behavior;

    /**
     * 行为商品
     */
    private String operatedGoods;

    /**
     * 行为发生时间
     */
    private Long time;

}
