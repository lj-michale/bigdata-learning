package com.bidata.example.kyro

import org.apache.spark.sql.SparkSession

object KryoExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("BasicOperatorExample")
      .getOrCreate()
    spark.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.kryo.registrator","YourKryoRegistrator.class.getName()")


    spark.close()

  }

}
