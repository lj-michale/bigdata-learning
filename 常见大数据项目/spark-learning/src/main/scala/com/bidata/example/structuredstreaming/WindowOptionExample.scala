package com.bidata.example.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.TimestampType

/**
 * Created by
 * Structured Streaming 窗口函数操作例子
 * 基于事件时间的wordcount
 * socket：nc -lk 9090
 */
object WindowOptionExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    // Read stream form socket,The data format is: 1553235690:dog cat|1553235691:cat fish
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9090)
      .load()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // Transform socket lines to DataFrame of schema { timestamp: Timestamp, word: String }
    val count = lines.as[String].flatMap(line => {
      val lineSplits = line.split("[|]")
      lineSplits.flatMap(item => {
        val itemSplits = item.split(":")
        val t = itemSplits(0).toLong
        itemSplits(1).split(" ").map(word => (t, word))
      })
    }).toDF("time", "word")
      .select($"time".cast(TimestampType), $"word")
      // Group the data by window and word and compute the count of each group
      .groupBy(
        window($"time", "10 minutes", "5 minutes"),
        $"word"
      ).count()

    val query = count.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("truncate", value = false)
      .start()

    query.awaitTermination()
  }
}
