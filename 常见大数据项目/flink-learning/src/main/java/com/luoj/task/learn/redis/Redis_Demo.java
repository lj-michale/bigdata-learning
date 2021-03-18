package com.luoj.task.learn.redis;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * @program flink-demo
 * @description: 演示flink-connectors-第三方提供的RedisSink
 * @author: lj
 * @create: 2021/03/04 10:43
 */
@Slf4j
public class Redis_Demo {

    public static void main(String[] args) throws Exception {

        // TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // TODO 2.source
        DataStreamSource<String> lines = env.socketTextStream("172.17.11.26", 9999);

        // TODO 3.transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(" ");
                for (String word : arr) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).sum(1);

        // TODO 4.sink
        result.print();

        // /opt/app/redis/redis/bin/redis-server  /opt/app/redis/redis-5.0.4/redis.conf
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("172.17.8.26").setDatabase(0).setPort(6379).build();
        RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<>(flinkJedisPoolConfig, new MyRedisMapper());
        result.addSink(redisSink);

        // TODO 5.execute
        env.execute();

    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            // 选择的数据结构是 key:String("wcresult") - value:hash(单词：数量) 命令：hSet
            return new RedisCommandDescription(RedisCommand.HSET,"wcresult");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> t) {
            log.info(" >>>>>>>>>>> key:{}", t.f0);
            return t.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> t) {
            log.info(" >>>>>>>>>>> value:{}", t.f1);
            return t.f1.toString();
        }
    }
}
