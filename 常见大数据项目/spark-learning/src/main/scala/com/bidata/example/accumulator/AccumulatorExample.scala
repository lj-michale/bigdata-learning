package com.bidata.example.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AccumulatorExample {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession
      .builder
      .appName("BasicOperatorExample")
      .getOrCreate()
    val conf = new SparkConf
    val sc = new SparkContext(conf)

    // 累加器：LongAccumulator、DoubleAccumulator、CollectionAccumulator、自定义累加器
    val fieldAcc = new FieldAccumulator
    sc.register(fieldAcc, "fieldAcc")

    val tableRDD = sc.textFile("table.csv").filter(_.split(",")(0) != "A")
    tableRDD.map(x => {
      val fields = x.split(",")
      val a = fields(1).toInt
      val b = fields(2).toLong
      fieldAcc.add(SumAandB(a, b))
      x
    }).count()

    spark.close()

  }
}
