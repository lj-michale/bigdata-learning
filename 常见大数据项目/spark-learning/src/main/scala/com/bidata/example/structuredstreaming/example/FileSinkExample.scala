package com.bidata.example.structuredstreaming.example

import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
 * @author :
 *       Spark Structured 内置Sink用例
 */
object FileSinkExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val source = spark.readStream
      .format("rate")
      // 每秒生成的行数，默认值为1
      .option("rowsPerSecond", 10)
      .option("numPartitions", 10)
      .load()

    val fileSink = source.writeStream
      .format("parquet")
      //.format("csv")
      //.format("orc")
      // .format("json")
      .option("path", "data/sink")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()

    fileSink.awaitTermination()
  }
}