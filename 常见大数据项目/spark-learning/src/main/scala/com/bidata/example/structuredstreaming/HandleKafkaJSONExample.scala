package com.bidata.example.structuredstreaming

import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Created by
 * Structured 解析Kafka数据
 */
object HandleKafkaJSONExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "structured-json-data")
      .option("maxOffsetsPerTrigger", 1000)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      .load()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val schema = StructType(Seq(
      StructField("deviceId", StringType),
      StructField("deviceName", StringType),
      StructField("deviceValue", DoubleType),
      StructField("deviceTime", LongType)
    ))

    val query = source
      .select(from_json($"value".cast("String"), schema).as("json")).select("json.*")
      .writeStream
      .outputMode("update")
      .format("console")
      //.option("checkpointLocation", checkpointLocation)
      .option("truncate", value = false)
      .start()

    query.awaitTermination()

  }

  def handleJson(json: String): Device = {
    val gson = new Gson()
    gson.fromJson(json, classOf[Device])
  }

  case class Device(deviceId: String, deviceName: String, deviceValue: Double, deviceTime: Long)

}

