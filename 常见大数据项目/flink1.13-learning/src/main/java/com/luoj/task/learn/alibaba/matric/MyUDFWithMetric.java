package com.luoj.task.learn.alibaba.matric;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author lj.michale
 * @description
 * @date 2021-12-21
 */
public class MyUDFWithMetric extends ScalarFunction {

    private transient Counter eventCounter;
    private transient Histogram valueHistogram;

    @Override
    public void open(FunctionContext context) throws Exception {
        eventCounter = context.getMetricGroup().counter("udf_events");
        valueHistogram = context
                .getMetricGroup()
                .histogram("udf_value_histogram", new DescriptiveStatisticsHistogram(10_000));

    }

    public Integer eval(Integer event) {
        eventCounter.inc();
        System.out.println(String.format("udf_events[%d]", eventCounter.getCount()));
        valueHistogram.update(event);
        return event;
    }

}
