package com.bigdata.pipeline.etl

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkEtlExample002 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("SparkEtlExample002")
      .setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val fileRDD = sc.textFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark3.2-learning\\dataset\\app.log")


  }

}
