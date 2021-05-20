package com.luoj.task.learn.async;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-20
 */
public class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {

    private long[] sleep = {100L, 1000L, 5000L, 2000L, 6000L, 100L};

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(final Integer input, final ResultFuture<String> resultFuture) {
        System.out.println(System.currentTimeMillis() + "-input:" + input + " will sleep " + sleep[input] + " ms");

        query(input, resultFuture);
    }

    private void query(final Integer input, final ResultFuture<String> resultFuture) {
        try {
            Thread.sleep(sleep[input]);
            resultFuture.complete(Collections.singletonList(String.valueOf(input)));
        } catch (InterruptedException e) {
            resultFuture.complete(new ArrayList<>(0));
        }
    }

    private void asyncQuery(final Integer input, final ResultFuture<String> resultFuture) {
        CompletableFuture.supplyAsync(new Supplier<Integer>() {

            @Override
            public Integer get() {
                try {
                    Thread.sleep(sleep[input]);
                    return input;
                } catch (Exception e) {
                    return null;
                }
            }
        }).thenAccept((Integer dbResult) -> {
            resultFuture.complete(Collections.singleton(String.valueOf(dbResult)));
        });
    }
}
