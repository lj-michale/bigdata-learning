package com.bidata.example.demos

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Shi shuai RollerQing on 2019/12/24 19:47
 * 加监控 获取Metrics信息
 */

object Kafka010Demo06 {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getCanonicalName}")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.addStreamingListener(new MyStreamingListener) //需要自己实现一个StreamingListener

    //读数据
    val groupID = "SparkKafka010"
    val topics = List("topicB")
    val kafkaParams: Map[String, String] = MyKafkaUtils.getKafkaConsumerParams(groupID, "false")
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    ds.foreachRDD(rdd => {
      //代表对数据进行处理
      if(! rdd.isEmpty())
        println(rdd.count())

      //实时监控
      val recordsCount = rdd.count()
      //monitor(ssc.sparkContext, recordsCount)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  //spark 2.2.0以上版本
  class MyStreamingListener extends StreamingListener {//有好多方法可以重写 可以看看
    //Called when processing of a batch of jobs has completed.当一批作业处理完成时调用。
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      val schedulingDelay = batchCompleted.batchInfo.schedulingDelay
      val processingDelay: Option[Long] = batchCompleted.batchInfo.processingDelay
      val processingStartTime: Long = batchCompleted.batchInfo.processingStartTime.get
      val numRecords: Long = batchCompleted.batchInfo.numRecords
      val batchTime: Time = batchCompleted.batchInfo.batchTime

      println(s"schedulingDelay = $schedulingDelay")
      println(s"processingDelay = $processingDelay")
      println(s"processingStartTime = $processingStartTime")
      println(s"numRecords = $numRecords")
      println(s"batchTime = $batchTime")
    }
  }

  def getKafkaConsumerParams(grouid: String = "SparkStreaming010", autoCommit: String = "true"): Map[String, String] = {
    val kafkaParams = Map[String, String] (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> autoCommit,
      //ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",//earliest、 none 、latest 具体含义可以点进去看
      ConsumerConfig.GROUP_ID_CONFIG -> grouid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )
    kafkaParams
  }
}

