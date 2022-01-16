package com.bidata.example.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    // 建立和spark框架的连接
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    //  val sc = SparkSession.builder().master("local").appName("WordCount").getOrCreate()

    // 1、 读取文件，一行行的文本
    val lines: RDD[String] = sc.textFile("datas").cache()
    // 2、扁平化ø
    val words: RDD[String] = lines.flatMap((_: String).split(" "))
    // 3、分组
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy((word: String) => word)
    // 4、计数
    val wordCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 5、将转换结果采集到控制台打印出来
    wordCount.collect().foreach(println)


    sc.stop()
  }

}
