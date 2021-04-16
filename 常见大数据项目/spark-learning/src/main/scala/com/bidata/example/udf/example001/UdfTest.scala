package com.bidata.example.udf.example001

import org.apache.spark.sql.{DataFrame, SparkSession}

object UdfTest {

  def main(args: Array[String]): Unit = {
    //  spark 上下文
    val spark: SparkSession = SparkSession.builder().appName("testSql").master("local").getOrCreate()
    //  日志级别
    spark.sparkContext.setLogLevel("error")
    //  读文件，创建表
    val df: DataFrame = spark.read.json("file:///E:/OpenSource/GitHub/bigdata-learning/常见大数据项目/spark-learning/data/people.json")
    df.createOrReplaceTempView("person")
    //  sql 语句
    val dfPerson: DataFrame = spark.sql("select name,age from person")
    dfPerson.show()
    //  udf :转大写字母
    spark.udf.register("toBig",(str:String) => str.toUpperCase)
    spark.sql("select toBig(name),age from person").show()

    spark.close()
  }

}
