package com.aurora.feature.func.process;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @descri 定时处理算子
 *
 * @author lj.michale
 * @date 2022-03-31
 */
public class TimerProcessFunction extends KeyedProcessFunction<String, String, String> {

    @Override
    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
        context.timerService().registerProcessingTimeTimer(50);
        String out = "hello " + s;
        collector.collect(out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
          // 到达时间点触发事件操作
        out.collect(String.format("Timer triggered at timestamp %d", timestamp));
    }

}