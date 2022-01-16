package com.bidata.example.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkRDDPartitioner {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxx"),
      ("cba", "xxxxxxx"),
      ("wnba", "xxxxxxx"),
      ("nba", "xxxxxxx")
    ))

    // 使用自定义分区器
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output")

    sc.stop()
  }

  /**
   * 自定义分区器
   */
  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3
    // 根据数据的key值返回分区数据的分区索引(从0开始)
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
      //      if (key == "nba") {
      //        0
      //      } else if ("cba" == key) {
      //        1
      //      }else {
      //        2
      //      }
    }
  }

}
