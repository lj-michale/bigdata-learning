package com.luoj.task.learn.matric.histogram;

import com.codahale.metrics.Histogram;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class MyMapper extends RichMapFunction<Long, Long> {
    private transient Histogram histogram;

    @Override
    public void open(Configuration config) {
//        this.histogram = getRuntimeContext()
//                .getMetricGroup()
//                .histogram("myHistogram", new MyHistogram());
    }

    @Override
    public Long map(Long value) throws Exception {
        this.histogram.update(value);
        return value;
    }
}