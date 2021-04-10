package com.luoj.task.learn.sink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


object SinkToRedisByScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textDStream = env.socketTextStream("",9000)
    val wordDStream = textDStream.map(new MapFunction[String, Tuple2[String, String]]() {
      @throws[Exception]
      override def map(s: String) = new Tuple2[String, String]("1_words", s)
    })

    import org.apache.flink.api.scala._
    val flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setDatabase(0).setHost("hadoop-01").setPort(6379).build
    val redisSink = new RedisSink[Tuple2[String, String]](flinkJedisPoolConfig, new SinkToRedisLearn.MyRedisMapper)

    wordDStream.addSink(redisSink)

  }

  class MyRedisMapper extends RedisMapper[Tuple2[String, String]] {
    override def getCommandDescription = new RedisCommandDescription(RedisCommand.LPUSH)
    override def getKeyFromData(data: Tuple2[String, String]): String = data._1
    override def getValueFromData(data: Tuple2[String, String]): String = data._2
  }

}
