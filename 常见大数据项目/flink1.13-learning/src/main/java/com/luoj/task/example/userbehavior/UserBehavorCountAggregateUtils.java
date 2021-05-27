package com.luoj.task.example.userbehavior;


import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author lj.michale
 * @description 增量计算用户点击行为数量
 * @date 2021-05-27
 */
public class UserBehavorCountAggregateUtils implements AggregateFunction<UserBehavingInfo, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    //一条数据执行一次
    @Override
    public Integer add(UserBehavingInfo UserBehavingInfo, Integer integer) {
        return integer + 1;

    }

    //窗口结束执行一次
    @Override
    public Integer getResult(Integer integer) {
        return integer;

    }

    @Override
    public Integer merge(Integer integer, Integer acc1) {
        return integer+acc1;

    }

}