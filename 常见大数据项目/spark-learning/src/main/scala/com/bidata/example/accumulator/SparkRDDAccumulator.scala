package com.bidata.example.accumulator

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDAccumulator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    var sum = 0;
    rdd.foreach(
      num => {sum += num}
    )

    // 期望结果是10， 实际确是0。原因executor分布式计算，没有将sum结果传递给driver端的sum
    println(sum)
    sc.stop()

  }

}
