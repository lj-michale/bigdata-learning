package com.bigdata.kafka

import java.util.Properties

import com.turing.common.PropertiesUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @descr Spark-Kafka工具类
 *
 * @author lj.michale
 * @date 2022-04-21
 */
object SparkKafkaUtils {

  private val properties: Properties = PropertiesUtils.getProperties("common-test.properties")
  val broker_list: String = properties.getProperty("kafka.broker.url")

  //////////////////////// kafka消费者配置
  var kafkaParam = collection.mutable.Map(
    // 用于初始化链接到集群的地址
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    // 用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall_consumer_group",
    // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    // 可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    // 如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    // 如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /////////////////////// 创建DStream，返回接收到的输入数据
  // LocationStrategies：根据给定的主题和集群地址创建consumer
  // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
  // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe：订阅一系列主题

  def getKafkaStream(topic: String,
                     ssc:StreamingContext ): InputDStream[ConsumerRecord[String, String]]={
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam ))
    dStream
  }

  def getKafkaStream(topic: String,
                     ssc:StreamingContext,
                     groupId:String): InputDStream[ConsumerRecord[String, String]]={
    kafkaParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam ))
    dStream
  }

  def getKafkaStream(topic: String,
                     ssc: StreamingContext,
                     offsets: Map[TopicPartition, Long],
                     groupId: String): InputDStream[ConsumerRecord[String, String]]={
    kafkaParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
    dStream
  }

}
