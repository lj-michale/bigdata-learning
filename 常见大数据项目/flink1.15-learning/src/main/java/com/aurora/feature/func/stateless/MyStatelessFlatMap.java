package com.aurora.feature.func.stateless;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
/**
 * @descri 无状态算子
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