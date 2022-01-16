package com.bidata.example.accumulator

import org.apache.spark.{SparkConf, SparkContext}

object Spark2RDDAccumulator {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    // 系统自带的分布式累加器
    var sum = sc.longAccumulator("sum");
    rdd.foreach(
      num => sum.add(num)

    )
    // 期望结果是10
    println("sum = " + sum.value)
    sc.stop()
  }

}
