package com.luoj.task.example.userbehavior;



import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */

public class GetUserInfoByUserIdAsyncFunction extends RichAsyncFunction<User, User> {

    private transient SQLClient sqlClient;

    private transient volatile Cache<Long, User> userInfoCache;

    @Override
    public void open(Configuration parameters) {
        // 异步读取 mysql client
        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(10).setEventLoopPoolSize(5));
        JsonObject config = new JsonObject()
                .put("url", "jdbc:mysql://localhost:3306/bigdata?serverTimezone=UTC")
                .put("driver_class", "com.mysql.jdbc.Driver")
                .put("max_pool_size", 20)
                .put("user", "root")
                .put("password", "root");

        sqlClient = JDBCClient.createShared(vertx, config);
        // 做缓存，优先读取缓存中数据，没有读到时，再从mysql中获取
        userInfoCache = CacheBuilder.newBuilder()
                .initialCapacity(500)  // 初始化缓存池大小
                .maximumSize(1000) // 缓存池最大为1000
                .expireAfterWrite(5, TimeUnit.MINUTES).build(); // 写入的数据5分钟后过期，表示失效
    }

    @Override
    public void asyncInvoke(User inputUser, ResultFuture<User> resultFuture) {

        long userId = inputUser.getUserId();
        // 从缓存中读取该用户的信息
        User userFromCache = userInfoCache.getIfPresent(userId);
        // 判断读取是否成功
        if (userFromCache != null && userFromCache.getLastLoadTime()!=null && userFromCache.getRegisterTime()!=null) {
            // 从缓存中成功获取到数据，user 补充时间字段的值
            inputUser.setLastLoadTime(userFromCache.getLastLoadTime());
            inputUser.setRegisterTime(userFromCache.getRegisterTime());
            resultFuture.complete(Collections.singletonList(inputUser));
        } else {
            // 查询的sql，此处需要注意查询的条件尽量打在索引上，避免查询超时
            String sql = "select `lastLoadTime`,`registerTime` from `mall_user` where `userId` = '" + userId +  "'";
            sqlClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
                @Override
                public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                    SQLConnection conn = sqlConnectionAsyncResult.result();
                    try {
                        conn.query(sql, resultSetAsyncResult -> {
                            if (resultSetAsyncResult.failed()) {
                                // 获取结果失败，重新执行。此处需要注意控制失败查询次数
                                asyncInvoke(inputUser, resultFuture);
                                return;
                            }
                            // 查询成功
                            ResultSet result = resultSetAsyncResult.result();
                            // 判断取到的值是否有记录
                            if (result.getRows() != null && result.getNumRows() > 0) {
                                User userFromDB = new User();
                                for (JsonObject userInfoFromDB : result.getRows()) {
                                    userFromDB.setUserId(userId);
                                    userFromDB.setRegisterTime(userInfoFromDB.getLong("registerTime"));
                                    userFromDB.setLastLoadTime(userInfoFromDB.getLong("lastLoadTime"));
                                }
                                // 存入 cache
                                userInfoCache.put(userId, userFromDB);
                                // user 补充时间字段的值
                                inputUser.setLastLoadTime(userFromDB.getLastLoadTime());
                                inputUser.setRegisterTime(userFromDB.getRegisterTime());
                            }
                            // 返回数据
                            resultFuture.complete(Collections.singletonList(inputUser));
                        });
                    } catch (Exception e) {
                        // 出现异常，也是需要将数据吐出的，不然会导致异步线程阻塞
                        resultFuture.complete(Collections.singletonList(inputUser));
                        System.out.println("数据查询异常 " + e.getMessage());
                    }
                    conn.close();
                }
            });
        }
    }

    @Override
    public void timeout( User input,  ResultFuture<User> resultFuture) {
        // 查询超时的数据，也需要返回出来，不然会导致数据丢失
        resultFuture.complete(Collections.singletonList(input));
    }

    @Override
    public void close() {
        sqlClient.close();
        userInfoCache.invalidateAll();
    }
}