package com.bidata.example.sparkcore

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.sum

object FancyApp {

  def appMain(args: Array[String]) = {
    // configure spark
    val spark = SparkSession
      .builder
      .appName("parjobs")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext.parallelize(1 to 100).toDF
    doFancyDistinct(df, "hdfs:///dis.parquet")
    doFancySum(df, "hdfs:///sum.parquet")

  }

  def doFancyDistinct(df: DataFrame, outPath: String) = df.distinct.write.parquet(outPath)

  def doFancySum(df: DataFrame, outPath: String) = df.agg(sum("value")).write.parquet(outPath)

}