package com.bidata.example.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.TimestampType

/**
 * Created by
 * Structured Streaming 窗口函数操作例子
 * 基于事件时间的wordcount,设置watermark
 * socket：nc -lk 9090
 */
object WindowOptionWithWatermarkExample {
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

    import org.apache.spark.sql.functions._
    import spark.implicits._

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
      .withWatermark("time", "10 minutes")
      // Group the data by window and word and compute the count of each group
      .groupBy(
        window($"time", "10 minutes", "5 minutes"),
        $"word"
      ).count()
    val query = count.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", value = false)
      .start()
    query.awaitTermination()
  }
}
/*
输入：1553227560:dog|1553227680:owl
-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------------+----+-----+
|window                                    |word|count|
+------------------------------------------+----+-----+
|[2019-03-22 12:05:00, 2019-03-22 12:15:00]|dog |1    |
|[2019-03-22 12:05:00, 2019-03-22 12:15:00]|owl |1    |
|[2019-03-22 12:00:00, 2019-03-22 12:10:00]|owl |1    |
|[2019-03-22 12:00:00, 2019-03-22 12:10:00]|dog |1    |
+------------------------------------------+----+-----+


输入：1553227740:cat|1553228040:dog
-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+----+-----+
|window                                    |word|count|
+------------------------------------------+----+-----+
|[2019-03-22 12:05:00, 2019-03-22 12:15:00]|dog |2    |
|[2019-03-22 12:05:00, 2019-03-22 12:15:00]|cat |1    |
|[2019-03-22 12:00:00, 2019-03-22 12:10:00]|cat |1    |
|[2019-03-22 12:10:00, 2019-03-22 12:20:00]|dog |1    |
+------------------------------------------+----+-----+


输入：1553228160:cat|1553227680:dog|1553227980:owl
-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+----+-----+
|window                                    |word|count|
+------------------------------------------+----+-----+
|[2019-03-22 12:05:00, 2019-03-22 12:15:00]|dog |3    |
|[2019-03-22 12:05:00, 2019-03-22 12:15:00]|owl |2    |
|[2019-03-22 12:15:00, 2019-03-22 12:25:00]|cat |1    |
|[2019-03-22 12:10:00, 2019-03-22 12:20:00]|owl |1    |
|[2019-03-22 12:10:00, 2019-03-22 12:20:00]|cat |1    |
|[2019-03-22 12:00:00, 2019-03-22 12:10:00]|dog |2    |
+------------------------------------------+----+-----+

输入：1553228460:owl|1553227440:fish|1553228220:owl
-------------------------------------------
Batch: 3
-------------------------------------------
+------------------------------------------+------+-----+
|window                                    |word  |count|
+------------------------------------------+------+-----+
|[2019-03-22 12:20:00, 2019-03-22 12:30:00]|owl   |1    |
|[2019-03-22 12:10:00, 2019-03-22 12:20:00]|owl   |2    |
|[2019-03-22 12:00:00, 2019-03-22 12:10:00]|fish  |1    |
|[2019-03-22 12:15:00, 2019-03-22 12:25:00]|owl   |2    |
+------------------------------------------+------+-----+

输入：1553227440:lion|1553228820:owl
-------------------------------------------
Batch: 4
-------------------------------------------
+------------------------------------------+----+-----+
|window                                    |word|count|
+------------------------------------------+----+-----+
|[2019-03-22 12:25:00, 2019-03-22 12:35:00]|owl |1    |
|[2019-03-22 12:20:00, 2019-03-22 12:30:00]|owl |2    |
+------------------------------------------+----+-----+
 */