package com.luoj.task.example.userbehavior;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author lj.michale
 * @description
 * @date 2021-05-27
 */
public class UserBehaviorRedisMapper implements RedisMapper<Tuple2<String,String>> {

    //设置redis 命令
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.ZADD,"rank:click");
    }

    //从数据中获取key
    @Override
    public String getKeyFromData(Tuple2 stringEventDetailsTuple2) {
        return String.valueOf(stringEventDetailsTuple2.f0);

    }

    //从数据中获取value
    @Override
    public String getValueFromData(Tuple2 stringEventDetailsTuple2) {
        return String.valueOf(stringEventDetailsTuple2.f1);

    }
}
