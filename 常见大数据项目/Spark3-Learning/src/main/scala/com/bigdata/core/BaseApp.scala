package com.bigdata.core

import com.bigdata.kafka.{KafkaOffsetManagerUtils, SparkKafkaUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JDouble, JInt, JLong, JString}

import scala.collection.mutable.ListBuffer

abstract class BaseApp {

  //消费者组和主题
  val master: String
  val appName: String
  val groupId: String
  val topic: String
  val bachTime: Int

  val toLong: CustomSerializer[Long] = new CustomSerializer[Long](ser = format => ({
    case JString(s) => s.toLong
    case JInt(s) => s.toLong
  },{
    case s:Long => JLong(s)
  }))
  val toDouble = new CustomSerializer[Double](ser = format => ({
    case JString(s) => s.toDouble
    case JDouble(s) => s.toDouble
  },{
    case s:Long => JDouble(s)
  }))

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(bachTime))

    val offsets: Map[TopicPartition, Long] = KafkaOffsetManagerUtils.getOffset(groupId, topic)
    val offsetRanges = ListBuffer.empty[OffsetRange]

    val sourceStream: DStream[String] = SparkKafkaUtils
      .getKafkaStream(topic, ssc, offsets, groupId)
      .transform(rdd => {
        val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges]
          .offsetRanges
        offsetRanges.clear()
        offsetRanges ++= newOffsetRanges
        rdd
      })
      .map(_.value())

    run(ssc, offsetRanges, sourceStream)

    ssc.start()
    ssc.awaitTermination()

  }

  // 具体的业务逻辑实现
  def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[String])

}