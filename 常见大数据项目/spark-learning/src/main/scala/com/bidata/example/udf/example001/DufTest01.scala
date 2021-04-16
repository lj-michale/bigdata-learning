package com.bidata.example.udf.example001

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

// udaf ： 多行变一行 ( 求平均数 ) ： 自定义类
object DufTest01 {

  def main(args: Array[String]): Unit = {

    //  spark 上下文
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkKuduTest").setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //  日志级别
    spark.sparkContext.setLogLevel("error")

    //  读文件，创建表
    val df: DataFrame = spark.read.json("file:///E:/OpenSource/GitHub/bigdata-learning/常见大数据项目/spark-learning/data/employees.json")
    df.createOrReplaceTempView("employee")

    //  引入隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //  注册函数
    spark.udf.register("myAverage",new MyAverage)
    val df2: DataFrame = spark.sql("select myAverage(salary) as avgSal from employee")
    df2.show()
    spark.close()

  }

}