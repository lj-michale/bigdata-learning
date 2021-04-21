package com.bidata.example.structuredstreaming.example

import org.apache.spark.sql.SparkSession

/**
 * @author :
 * @date :
 *       基于Socket的数据源 nc -lc 9090
 */
object SocketSourceExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9090)
      .load()

    val query = lines.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .start()

    query.awaitTermination()
  }
}
