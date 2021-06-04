package com.bidata.example.structuredstreaming

import java.sql.Timestamp

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object MultiQueryCache {

  val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("StructuredStreamingTest")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    //    spark.conf.set("spark.sql.session.timeZone", "GMT+8")

    import spark.implicits._

    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.16.19:9092")
      .option("subscribe", "test_pressure")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    spark.sparkContext.setLogLevel("WARN")

    //    val lines = spark.readStream
    //      .format("socket")
    //      .option("host", "192.168.16.17")
    //      .option("port", 9999)
    //      .load()

    val dataSet = lines.map(line => {
      //      println("line===>", line)
      val arr = line.split(";")
      PaasInput(arr(0), arr(1), arr(2), arr(3), this.formatTime2TimeStamp(arr(4)))
    }).as[PaasInput]
      .withWatermark("inputtime", "2 minutes")
      .createOrReplaceTempView("paas_events")

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
    val query1 = spark.sql("""select
                             |app_id,
                             |max(aid) as aid,
                             |uid,
                             |CAST(window.start AS STRING) AS satrt_time,
                             |CAST(window.end AS STRING) AS end_time
                             |from paas_events
                             |group by app_id, uid, window(inputtime, '1 minutes')""".stripMargin)
      .writeStream
      .format("console")
      .queryName("query1")
      .outputMode(OutputMode.Append())
      .start()

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")
    val query2 = spark.sql("""select
                             |max(app_id) as app_id,
                             |aid,
                             |s,
                             |CAST(window.start AS STRING) AS satrt_time,
                             |CAST(window.end AS STRING) AS end_time
                             |from paas_events
                             |group by aid, s, window(inputtime, '1 minutes')""".stripMargin)
      .writeStream
      .format("console")
      .queryName("query2")
      .outputMode(OutputMode.Append())
      .start()

    spark.streams.awaitAnyTermination()

  }

  def formatTime2TimeStamp(timeStr: String): Timestamp ={
    val milliSeconds = dateFormat.parse(timeStr).getTime
    val timestamp = new Timestamp(milliSeconds)

    timestamp
  }

  case class PaasInput(str: String, str1: String, str2: String, str3: String, timestamp: Timestamp)

}
