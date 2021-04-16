package com.bidata.example.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession

object ReadKafkaBySparkStreaming extends Serializable {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf();
    conf.setMaster("local")
    conf.setAppName("wangjk")
    conf.set("spark.testing.memory", "2147480000")

    val ssc =  new StreamingContext(conf, Seconds.apply(5))
    val spark = SparkSession.builder().config(conf).getOrCreate

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "datatwo:9092,datathree:9092,datafour:9092",// kafka 集群
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "dsffaa",
      "auto.offset.reset" -> "earliest",  // 每次都是从头开始消费（from-beginning），可配置其他消费方式
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("air")  //主题，可配置多个
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val rd2=stream.map(e=>(e.value()))  //e.value() 是kafka消息内容，e.key为空值
    rd2.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
