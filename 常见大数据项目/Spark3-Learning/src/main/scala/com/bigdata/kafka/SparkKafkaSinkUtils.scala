package com.bigdata.kafka

import java.util.Properties

import com.turing.common.PropertiesUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @descr Spark Kafka Sink 工具类
 *
 * @author lj.michale
 * @date 2022-04-21
 */
object SparkKafkaSinkUtils {

  private val properties: Properties = PropertiesUtils.getProperties("common-test.properties")
  val brokerUrl:String = properties.getProperty("kafka.broker.list")

  var kafkaProducer: KafkaProducer[String, String] = null

  def createKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put("bootstrap.servers", brokerUrl)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idompotence",(true: java.lang.Boolean))
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  def send(topic: String,key:String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
  }

}
