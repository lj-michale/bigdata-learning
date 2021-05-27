package com.luoj.task.example.userbehavior;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class UserBehavorCountWindowFunction extends ProcessWindowFunction<String,Tuple2<String, String>,String,TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<String> iterable, Collector<Tuple2<String, String>> collector) throws Exception {
        collector.collect(new Tuple2(key, iterable.iterator().next()));
    }
}