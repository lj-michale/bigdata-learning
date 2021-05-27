package com.luoj.task.learn.matric.counter;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

/**
 * @author lj.michale
 * @description
 * Counter #
 * A Counter is used to count something. The current value can be in- or decremented using inc()/inc(long n) or dec()/dec(long n). You can create and register a Counter by calling counter(String name) on a MetricGroup.
 * @date 2021-05-27
 */
public class MyMapper extends RichMapFunction<String, String> {

    private transient Counter counter;

    @Override
    public void open(Configuration config) {
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("myCounter");
//         .counter("myCustomCounter", new CustomCounter());
    }

    @Override
    public String map(String value) throws Exception {
        this.counter.inc();
        return value;
    }
}

