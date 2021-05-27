package com.luoj.task.example.userbehavior;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class UserBehavingInfoKeyByGood implements KeySelector<UserBehavingInfo, String> {

    private static final long serialVersionUID = 4780234853172462378L;

    @Override
    public String getKey(UserBehavingInfo value) throws Exception {
        return value.getOperatedGoods();
    }
}
