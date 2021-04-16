package com.bidata.example.udf.example001

import org.apache.spark.sql.{DataFrame, SparkSession}

// udtf ： 炸裂成多行
object UdtfTestTest {
  def main(args: Array[String]): Unit = {
    //  spark 上下文
    val spark: SparkSession = SparkSession
      .builder
      .master("local")
      .appName("UserAnalysis")
      .enableHiveSupport()      //启用hive
      .getOrCreate()
    //  日志级别
    spark.sparkContext.setLogLevel("error")
    //  读取文件
    val df1: DataFrame = spark.read.json("file:///E:/OpenSource/GitHub/bigdata-learning/常见大数据项目/spark-learning/data/people.json")
    df1.createOrReplaceTempView("people")
    spark.sql("select * from people").show()
    println("=========================================")
    //  注册 utdf 算子，这里无法使用 sparkSession.udf.register()
    spark.sql("CREATE TEMPORARY FUNCTION zhalie as 'com.bidata.example.udf.example001.MyUdtf'")
    //  使用我们的算子
    spark.sql("select age,zhalie(name) from people").show()
    spark.close()
  }
}
