package com.bidata.example.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkWordCount3 {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().master("local").appName("WordCount").getOrCreate()
    val lines: RDD[String] = sc.read.textFile("datas").cache().rdd
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    // 相同key的数据，可以对value进行聚合
    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    // 5、将转换结果采集到控制台打印出来
    wordCount.collect().foreach(println)

    sc.stop()

  }

}
