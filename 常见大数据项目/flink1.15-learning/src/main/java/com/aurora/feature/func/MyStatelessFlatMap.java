package com.aurora.feature.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-03-31
 */
public class MyStatelessFlatMap implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String in, Collector<String> collector) throws Exception {
        String out = "hello " + in;
        collector.collect(out);
    }
}