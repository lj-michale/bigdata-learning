package com.bigdata.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * @descr SparkSQLExample01
 * @author lj.michale
 * @date 2021-06
 */
object SparkSQLExample01 {

  val logger = Logger.getLogger(SparkSQLExample01.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSQLExample01")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases").show()
    spark.sql("use report")
    spark.sql("show tables").show()
    spark.sql("select * from houshuai_test").show()

    spark.stop()

  }

}
