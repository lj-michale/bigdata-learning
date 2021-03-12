package com.bidata.demos

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQLDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("SparkSQLDemo").setMaster("local[*]")
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sql("show databases").show()


    sparkSession.stop()

  }

}
