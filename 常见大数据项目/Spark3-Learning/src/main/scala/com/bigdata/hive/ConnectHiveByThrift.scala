package com.bigdata.hive

import com.bigdata.utils.HiveUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object ConnectHiveByThrift {

  val logger = Logger.getLogger(ConnectHiveByThrift.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ConnectHiveByThrift")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    // 开启动态分区
    HiveUtil.openDynamicPartition(spark)
    // 开启压缩
    HiveUtil.openCompression(spark)

    spark.sql("show databases").show()

    spark.close()

  }

}
