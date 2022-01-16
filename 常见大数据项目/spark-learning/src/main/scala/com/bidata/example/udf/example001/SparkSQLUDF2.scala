package com.bidata.example.udf.example001

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQLUDF2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("udf")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("ageAvg", new MyAvgUDAF())
    spark.sql("select ageAvg(age) from user").show()

    spark.close()
  }

  /**
   * 自定义聚合函数类：计算年龄的平均值。
   * 1. 继承 UserDefinedAggregateFunction
   * 2. 重写方法
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction {
    // 输入数据结构
    override def inputSchema: StructType = {
      StructType(
        Array(StructField("age", LongType))
      )
    }

    // 缓冲区计算结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType), // 年龄和
          StructField("count", LongType) // 人数
        )
      )
    }

    // 函数计算结果的类型，out
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // scala 语法
      //      buffer(0) = 0L
      //      buffer(1) = 0l
      buffer.update(0, 0l)
      buffer.update(1, 0l)
    }

    // 根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    // 合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer2.getLong(0) + buffer1.getLong(0))
      buffer1.update(1, buffer2.getLong(1) + buffer1.getLong(1))
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }


}
