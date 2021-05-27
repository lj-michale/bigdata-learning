package com.luoj.task.example.userbehavior;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class AsyncInsertUserBehavior2Mysql extends RichAsyncFunction<UserBehavingInfo, UserBehavingInfo> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void asyncInvoke(UserBehavingInfo userBehavingInfo, ResultFuture<UserBehavingInfo> resultFuture) throws Exception {

    }


    @Override
    public void timeout(UserBehavingInfo input, ResultFuture<UserBehavingInfo> resultFuture) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
