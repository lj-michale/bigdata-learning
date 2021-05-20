package com.luoj.task.learn.async;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-20
 */
public class AsyncIODemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final int maxCount = 6;
        final int taskNum = 1;
        final long timeout = 40000;

        DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));
        AsyncFunction<Integer, String> function = new SampleAsyncFunction();

        DataStream<String> result = AsyncDataStream.unorderedWait(
                inputStream,
                function,
                timeout,
                TimeUnit.MILLISECONDS,
                10
        ).setParallelism(taskNum);

        result.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "," + System.currentTimeMillis();
            }
        }).print();

        env.execute("Async IO Demo");

    }

    private static class SimpleSource implements SourceFunction<Integer> {

        private volatile boolean isRunning = true;
        private int counter = 0;
        private int start = 0;

        public SimpleSource(int maxNum) {
            this.counter = maxNum;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while ((start < counter || counter == -1) && isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    System.out.println("send data:" + start);
                    ctx.collect(start);
                    ++start;
                }
                Thread.sleep(10L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }



}
