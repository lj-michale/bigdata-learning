package com.luoj.task.learn.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import scala.Tuple2;

/**
 * @author lj.michale
 * @description
 * @date 2021-04-10
 */
public class SinkToRedisLearn {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> lines = env.socketTextStream("192.168.***.***", 8888);

        // String -> Tuple2<String, String>
        DataStream<Tuple2<String, String>> wordDStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>("1_words", s);
            }
        });

        // Jedis Setting
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setDatabase(0).setHost("hadoop-01").setPort(6379).build();

        // create sink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(flinkJedisPoolConfig,new MyRedisMapper());
        wordDStream.addSink(redisSink);

        env.execute("SinkToRedisLearn");

    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data._1;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data._2;
        }
    }

}
