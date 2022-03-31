package com.aurora.feature.func;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-03-31
 */
public class MyStatelessMap implements MapFunction<String, String> {
    @Override
    public String map(String in) throws Exception {
        String out = "hello " + in;
        return out;
    }
}