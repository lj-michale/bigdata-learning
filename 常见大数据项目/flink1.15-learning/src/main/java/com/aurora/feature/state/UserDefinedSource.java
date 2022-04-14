package com.aurora.feature.state;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.util.Random;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-10
 */
public class UserDefinedSource implements SourceFunction<Jason> {

    @Override
    public void run(SourceContext<Jason> sourceContext) throws Exception {
        for (int i = 1; i <= 5000; ++i) {
        Jason jason = new Jason();
        jason.setName("li.michale");
        jason.setAge(100);
        sourceContext.collect(jason);
        }
    }

    @Override
    public void cancel() {

    }
}
