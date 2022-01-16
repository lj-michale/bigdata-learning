package com.bidata.example.udf.example001

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

object SparkSQLUDF3 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("udf")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    // 使用
//    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

    spark.sql("select ageAvg(age) from user").show()

    spark.close()
  }

  /**
   * 自定义聚合函数类：计算年龄的平均值。
   * 1. 继承 Aggregator
   * 2. 重写方法
   */
  case class Buf(var total:Long, var count:Long)
  class MyAvgUDAF extends Aggregator[Long, Buf, Long] {
    // zero 初始值或叫零值
    // 缓冲区初始化
    override def zero: Buf = {
      Buf(0l,0l)
    }

    // 根据输入的数据来更新缓冲区的数据
    override def reduce(b: Buf, a: Long): Buf = {
      b.total = b.total + a
      b.count = b.count + 1
      b
    }

    // 合并缓冲区
    override def merge(b1: Buf, b2: Buf): Buf = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }

    // 计算输出结果
    override def finish(reduction: Buf): Long = {
      reduction.total / reduction.count
    }

    // 序列化问题
    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buf] = Encoders.product

    // 输出结果的编码
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
