package com.luoj.task.learn.udf;

import avro.shaded.com.google.common.cache.Cache;
import avro.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 * @date 2021-08-05
 */
public class RandomFunction extends ScalarFunction {

    private static Cache<String, Integer> cache = CacheBuilder.newBuilder()
            .maximumSize(2)
            .expireAfterWrite(3, TimeUnit.SECONDS)
            .build();

    public int eval(String pvid) {
//        profileLog.error("RandomFunction invoked:" + atomicInteger.incrementAndGet());
        Integer result = cache.getIfPresent(pvid);
        if (null == result) {
            int tmp = (int)(Math.random() * 1000);
            cache.put("pvid", tmp);
            return tmp;
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        super.close();
        cache.cleanUp();
    }

}
