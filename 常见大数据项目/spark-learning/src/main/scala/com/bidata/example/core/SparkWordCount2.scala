package com.bidata.example.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkWordCount2 {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().master("local").appName("WordCount").getOrCreate()

    val lines: RDD[String] = sc.read.textFile("datas").cache().rdd
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)

    val wordCount: RDD[(String, Int)] = groupRdd.map {
      case (word, list) => {
        list.reduce((t1, t2) => {
          (t1._1, t1._2 + t2._2)
        })
      }
    }

    // 5、将转换结果采集到控制台打印出来
    wordCount.collect().foreach(println)

    sc.stop()

  }


}
