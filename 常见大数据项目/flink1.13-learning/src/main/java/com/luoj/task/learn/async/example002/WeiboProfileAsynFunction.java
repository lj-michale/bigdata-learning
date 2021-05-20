package com.luoj.task.learn.async.example002;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 *    WeiboProfileAsynFunction
 * @date 2021-05-20
 */
public class WeiboProfileAsynFunction extends RichAsyncFunction<Tuple2<Map<String, Object>,Map<String, Object>>, Tuple4<Boolean, String, String,String>> {

    /**
     * @descr
     * @param mapMapTuple2
     * @param resultFuture
     */
    @Override
    public void asyncInvoke(Tuple2<Map<String, Object>, Map<String, Object>> mapMapTuple2, ResultFuture<Tuple4<Boolean, String, String, String>> resultFuture) throws Exception {

    }

    /**
     * @descr
     * @param input
     * @param resultFuture
     */
    @Override
    public void timeout(Tuple2<Map<String, Object>, Map<String, Object>> input, ResultFuture<Tuple4<Boolean, String, String, String>> resultFuture) throws Exception {

    }
}
