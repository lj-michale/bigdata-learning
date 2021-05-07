package com.bidata.example.operator

import org.apache.spark.sql.SparkSession

object BasicOperatorExample {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("BasicOperatorExample")
      .getOrCreate()

    // 累加器：LongAccumulator、DoubleAccumulator、CollectionAccumulator、自定义累加器

  }


}
