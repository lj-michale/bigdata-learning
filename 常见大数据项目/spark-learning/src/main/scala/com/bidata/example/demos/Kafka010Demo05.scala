package com.bidata.example.demos

import com.alibaba.fastjson.JSONObject
import com.bidata.example.matric.SparkMetricsUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * 加监控 获取Metrics信息
 */

object Kafka010Demo05 {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getCanonicalName}")
    val ssc = new StreamingContext(conf, Seconds(5))
    //    ssc.addStreamingListener()

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
      monitor(ssc.sparkContext, recordsCount)
    })
    ssc.start()
    ssc.awaitTermination()
  }


  // 获取json串；解析json串；落地
  def monitor(sc: SparkContext, recordsCount: Long) = {
    //准备参数
    val appName = sc.appName
    val appId = sc.applicationId
    //注意这里：如果放到集群运行地址localhost可以换没问题
    //但是注意放在集群运行的spark程序可能不止一个，所以端口可能会被占用 就是说端口可能为4041 4042
    // 肯定有这样的情况 所以如果要用 还要再做优化
    val url = "http://localhost:4040/metrics/json"

    // 获取json串，转为jsonObject
    val metricsObject: JSONObject = SparkMetricsUtils.getMetricsJson(url).getJSONObject("gauges")

    //开始时间
    val startTimePath = s"$appId.driver.$appName.StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val startTime: JSONObject = metricsObject.getJSONObject(startTimePath)
    val processingStartTime = if(startTime != null)
      startTime.getLong("value")
    else -1L


    println(s"processingStartTime = $processingStartTime")

    // 每批次执行的时间；开始时间（友好格式）；处理的平均速度（记录数/时间）
    // lastCompletedBatch_processingDelay + schedulingDelay = 每批次执行的时间

    // 组织数据保存
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

