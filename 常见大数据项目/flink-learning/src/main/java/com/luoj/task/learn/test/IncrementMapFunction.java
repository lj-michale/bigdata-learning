package com.luoj.task.learn.test;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-12
 */
public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}