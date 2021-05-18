package com.bidata.example.hive

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._


object KafkaToHive{

  def main(args: Array[String]){

    val sparkConf = new SparkConf()
    val spark = SparkSession.builder.config(sparkConf)
                            .config("spark.driver.host", "localhost")
                            .config("hive.exec.dynamic.partition.mode","nonstrict")
                            .config("hive.exec.dynamic.partition","true")
                            .appName("FilePartitionTest")
                            .master("local")
                            .enableHiveSupport()
                            .getOrCreate
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop100:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "hucheng",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val topics = Array("test1")

    //读取Kafka数据创建DStream
    val kafkaDStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaPara))

    // 开始处理DStream,其中主要的业务写在里面
    kafkaDStream.foreachRDD(rdd => {

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // TODO 具体业务逻辑

        // 写入Hive ...
        val subRdd:RDD[Int] = rdd.sparkContext.parallelize(Seq(1,2,3,4))

        // 提交offset
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      })
  }
}