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

    sparkSession.sql("show databases").explain()

    sparkSession.sql(
      """
        |SELECT app_key,msg_id,uid,platform
        |FROM edw.third_msg_status_latest_hour_log
        |WHERE data_hour >= 2021020400
        |AND data_hour <= 2021020500
        |""".stripMargin).explain()



    sparkSession.stop()

  }

}
