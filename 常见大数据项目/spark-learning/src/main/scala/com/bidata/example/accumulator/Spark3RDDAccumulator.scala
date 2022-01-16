package com.bidata.example.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark3RDDAccumulator {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    // 系统自带的分布式累加器  分布式只写共享变量
    // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：
    // 一般情况下，累加器会放在行动算子中进行操作
    var sum = sc.longAccumulator("sum");
    val mapRDD: RDD[Unit] = rdd.map(
      num => sum.add(num)
    )

    mapRDD.collect()
    mapRDD.collect()

    // 期望结果是10
    println("sum = " + sum.value)
    sc.stop()
  }
}
