package com.luoj.task.learn.partition;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-10
 */
public class MyParalleSource implements ParallelSourceFunction<Long> {

    private long count = 1L;
    private Boolean isRunning = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(count);
            count++;
            // 每秒产生一条
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
