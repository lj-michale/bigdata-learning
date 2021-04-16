package com.bidata.example.sparkcore

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.concurrent.Executors
import org.apache.spark.sql.functions.sum
import scala.concurrent._
import scala.concurrent.duration._

object FancyAppAfterYouhua {

  def appMain(args: Array[String]) = {
    // configure spark
    val spark = SparkSession
      .builder
      .appName("parjobs")
      .getOrCreate()

    // Set number of threads via a configuration property
    val pool = Executors.newFixedThreadPool(5)

    // create the implicit ExecutionContext based on our thread pool
    import spark.implicits._
    implicit val xc = ExecutionContext.fromExecutorService(pool)
    val df = spark.sparkContext.parallelize(1 to 100).toDF
    val taskA = doFancyDistinct(df, "hdfs:///dis.parquet")
    val taskB = doFancySum(df, "hdfs:///sum.parquet")

    // Now wait for the tasks to finish before exiting the app
    Await.result(Future.sequence(Seq(taskA,taskB)), Duration(1, MINUTES))

  }

  def doFancyDistinct(df: DataFrame, outPath: String)(implicit xc: ExecutionContext) = Future {
    df.distinct.write.parquet(outPath)
  }

  def doFancySum(df: DataFrame, outPath: String)(implicit xc: ExecutionContext) = Future {
    df.agg(sum("value")).write.parquet(outPath)
  }

}
