package com.bigdata.common.utils

import java.util.Properties

import com.turing.common.PropertiesUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.bigdata.common.constant.GlobalConstant.Pipeline_Global_Prpos

/**
 * @descr KafkaSinkUtils
 *
 * @author lj.michale
 * @date 2022-05-20
 */
object KafkaSinkUtils {

  private val properties: Properties = PropertiesUtils.getProperties(Pipeline_Global_Prpos)
  val brokerUrl: String = properties.getProperty("kafka.broker.list")

  var kafkaProducer: KafkaProducer[String, String] = null

  /**
   * @descr createKafkaProducer
   */
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

  /**
   * @descr send to kafka
   *
   * @param topic
   * @param msg
   */
  def send(topic: String,
           msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer

    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * @descr send to kafka
   *
   * @param topic
   * @param key
   * @param msg
   */
  def send(topic: String,
           key:String,
           msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer

    kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
  }

}
