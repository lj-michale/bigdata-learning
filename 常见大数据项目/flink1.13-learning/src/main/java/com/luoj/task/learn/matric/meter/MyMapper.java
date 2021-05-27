package com.luoj.task.learn.matric.meter;

import com.codahale.metrics.Meter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class MyMapper extends RichMapFunction<Long, Long> {
    private transient Meter meter;

    @Override
    public void open(Configuration config) {
//        this.meter = getRuntimeContext()
//                .getMetricGroup()
//                .meter("myMeter", new MyMeter());
    }

    @Override
    public Long map(Long value) throws Exception {
//        this.meter.markEvent();
        return value;
    }
}