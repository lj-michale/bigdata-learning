package com.luoj.task.learn.matric.gauge;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class MyMapper extends RichMapFunction<String, String> {

    private transient int valueToExpose = 0;

    @Override
    public void open(Configuration config) {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("MyGauge", new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return valueToExpose;
                    }
                });
    }

    @Override
    public String map(String value) throws Exception {
        valueToExpose++;
        return value;
    }
}
