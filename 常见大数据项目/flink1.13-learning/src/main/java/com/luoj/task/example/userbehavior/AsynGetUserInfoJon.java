package com.luoj.task.example.userbehavior;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description  使用异步方式读取mysql数据
 * @date 2021-05-27
 */

public class AsynGetUserInfoJon {

    public static void main(String[] args) throws Exception {

        // 获取 flink env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 因为本地测试，并行度设置为1
        env.setParallelism(1);
        // 从 9980 端口读取数据，传入的数据为userid
        SingleOutputStreamOperator<User> localhost =
                env.socketTextStream("localhost", 9980, "\n")
                        .flatMap(new FlatMapFunction<String, User>() {
                            @Override
                            public void flatMap(String value, Collector<User> out) throws Exception {
                                try {
                                    long userId = Long.parseLong(value);
                                    User user = new User();
                                    user.setUserId(userId);
                                    out.collect(user); // 此时吐出的数据中只有userid
                                } catch (Exception e) {
                                    System.out.println("error "+ e);
                                }
                            }
                        });

        // 关联维表 mall_user
        SingleOutputStreamOperator<User> dataEntitySingleOutputStreamOperator = AsyncDataStream
                // 3,TimeUnit.SECONDS 表示超时时间为3秒
                .unorderedWait(localhost, new GetUserInfoByUserIdAsyncFunction(), 3, TimeUnit.SECONDS);

        // 数据打印
        dataEntitySingleOutputStreamOperator.print();

        env.execute("Flink_Async_IO_Test");
    }
}
