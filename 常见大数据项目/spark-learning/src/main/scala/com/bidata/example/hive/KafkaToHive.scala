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

    //将每条消息的KV取出
    val valueDStream = kafkaDStream.map(record => record.value())
    valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    val dStream = kafkaDStream.map((_, 1L))
      /**
       * updateStateByKey 是有状态计算方法
       * 第一个参数表示 相同key的value集合
       * 第二个参数表示 相同key的缓冲区的数据，有可能为空
       * 这里中间结果需要保存到检查点的位置中，需要设定检查点
       */
      .updateStateByKey(
        (seq: Seq[Long], buffer: Option[Long]) => {
          val newBufferValue = buffer.getOrElse(0L) + seq.sum
          Option(newBufferValue)
        }
      )

    // 开始处理DStream,其中主要的业务写在里面
    kafkaDStream.foreachRDD(rdd => {

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // TODO 具体业务逻辑
        val resDF:RDD[(ConsumerRecord[String, String], Int)] = rdd.map((_, 1)).reduceByKey(_ + _)

        // 写入Hive ...
        val subRdd:RDD[Int] = rdd.sparkContext.parallelize(Seq(1,2,3,4))

        // 提交offset
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()

  }
}