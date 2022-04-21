package com.bigdata.core

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.kafka.{KafkaOffsetManagerUtils, SparkKafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.ListBuffer

/**
 * @descr SparkStreaming 基础抽象类, 保证exactly-one
 *
 * @author lj.michale
 * @date 2022-04-21
 */
abstract class AbstractTuringBaseStreamApp {

  val master: String
  val appName: String
  val groupId: String
  val topic: String
  val bachTime: Int

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
        conf.set("spark.default.parallelism", "100")
        conf.set("spark.shuffle.consolidateFiles", "true")
        conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        conf.set("spark.streaming.backpressure.enabled", "true")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.default.parallelism", "15")
        conf.set("spark.storage.memoryFraction", "0.6")
        conf.set("spark.shuffle.file.buffer", "64")
        conf.set("spark.streaming.concurrentJobs", "15")
        conf.set("spark.streaming.kafka.maxRetries", "3")
        conf.set("spark.streaming.backpressure.pid.minRate","20000")
        conf.set("spark.streaming.kafka.maxRatePerPartition", "50000")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(bachTime))

    val beginOffset: Map[TopicPartition, Long] = KafkaOffsetManagerUtils.getOffset(groupId, topic)
    var startInputDstream: InputDStream[ConsumerRecord[String, String]] = null

    /**
     * @descr 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
     */
    if(beginOffset != null && beginOffset.size > 0) {
      startInputDstream = SparkKafkaUtils.getKafkaStream(topic,
        ssc,
        beginOffset,
        groupId)
    } else {
      startInputDstream  = SparkKafkaUtils.getKafkaStream(topic,ssc,groupId)
    }

    // 获得本批次偏移量的移动后的新位置
    var beginUpOffsetRanges: Array[OffsetRange] = null
    val beginUpInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = startInputDstream
      .transform { rdd =>
        beginUpOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    // 将数据转换为标准JSON格式
    val startJsonObjDstream: DStream[JSONObject] = beginUpInputGetOffsetDstream
      .map { record =>
        val jsonString: String = record.value()
        val jSONObject: JSONObject = JSON.parseObject(jsonString)
        jSONObject
      }

    // 进行数据业务逻辑处理
    run(ssc, beginOffset, startJsonObjDstream)

    // 处理完之后，将数据保存到Redis
    KafkaOffsetManagerUtils.saveOffset(groupId, topic, beginUpOffsetRanges)

    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * @descr sparkstreaming 数据业务逻辑处理实现
   *
   * @param ssc
   * @param beginOffset
   * @param sourceStream
   */
  def run(ssc: StreamingContext, beginOffset: Map[TopicPartition, Long], sourceStream: DStream[JSONObject])

}
