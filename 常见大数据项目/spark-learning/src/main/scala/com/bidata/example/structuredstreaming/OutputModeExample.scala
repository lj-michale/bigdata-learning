package com.bidata.example.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
 * Created by
 * Structured Streaming 三种输出模式的例子
 * 以wordcount为例
 * socket 命令:nc -lk 9090
 */
object OutputModeExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val line = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9090)
      .load()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val words = line.as[String].flatMap(_.split(" "))

    /**
     * 测试输出模式为complete
     * 整个更新的结果表将写入外部存储器
     * -------------------------------------------
     * 输入：dog cat
     * 结果：
     * +-----+-----+
     * |value|count|
     * +-----+-----+
     * |dog  |1    |
     * |cat  |1    |
     * +-----+-----+
     * -------------------------------------------
     * 输入：dog fish
     * 结果：
     * +-----+-----+
     * |value|count|
     * +-----+-----+
     * |dog  |2    |
     * |cat  |1    |
     * |fish |1    |
     * +-----+-----+
     * -------------------------------------------
     * 输入：cat lion
     * 结果：
     * +-----+-----+
     * |value|count|
     * +-----+-----+
     * |lion |1    |
     * |dog  |2    |
     * |cat  |2    |
     * |fish |1    |
     * +-----+-----+
     * -------------------------------------------
     */
    val completeQuery = words.groupBy("value").count().writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("truncate", value = false)
      .start()
    completeQuery.awaitTermination()

  }
}
