package com.aurora.feature.func.state;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @descri 有状态算子
 *
 * @author lj.michale
 * @date 2022-03-31
 */
@Slf4j
public class MyStatefulFlatMap extends RichFlatMapFunction<String, Long> {
    ValueState<Long> counterState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "Counter",
                Types.LONG
        );
        this.counterState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(String s, Collector<Long> collector) throws Exception {
        Long count = 0L;
        if (this.counterState.value() != null) {
            count = this.counterState.value();
        }
        count ++;
        this.counterState.update(count);
        collector.collect(count);
    }
}