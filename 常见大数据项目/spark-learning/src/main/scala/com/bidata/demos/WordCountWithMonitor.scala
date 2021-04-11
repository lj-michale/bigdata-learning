package com.bidata.demos


import org.apache.spark.{SparkConf, SparkContext}

object WordCountWithMonitor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getCanonicalName}")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

//    sc.addSparkListener(new SparkMonitoringListener)

    val lines = sc.textFile("C:\\Users\\wangsu\\Desktop\\流式项目课件2\\antispider24\\src\\main\\lua\\controller.lua")
    lines.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_+_)
      .collect()

    sc.stop()
  }
}

