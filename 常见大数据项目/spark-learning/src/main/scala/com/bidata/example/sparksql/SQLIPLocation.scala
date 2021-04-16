package com.bidata.example.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用SaprkSQL实现iplocation
 * Created by lq on 2018/9/29 17:04.
 */
object SQLIPLocation {

  val rulesFilePath = "f:\\data\\ip.txt"
  val accessFilePath = "f:\\data\\access.log"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("SQLIPLocation").master("local[*]").getOrCreate()

    //读取ip规则数据
    val ipRulesLine: Dataset[String] = spark.read.textFile(rulesFilePath)

    //整理IP规则数据
    import spark.implicits._
    val tpRDDs: Dataset[(Long, Long, String)] = ipRulesLine.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })

    val ipRulesDF: DataFrame = tpRDDs.toDF("start_num", "end_num", "province")
    //将IP规则数据注册成视图
    ipRulesDF.createTempView("v_ip_rules")

    //读取访问日志数据
    val accessLogLine: Dataset[String] = spark.read.textFile(accessFilePath)

    //整理访问日志数据
//    import cn.edu360.spark.day06.MyUtils
//    val ips: DataFrame = accessLogLine.map(line=> {
//      val fields = line.split("[|]")
//      val ip = fields(1)
////      MyUtils.ip2Long(ip)
//    }).toDF("ip")

    val ips:DataFrame = null

    //将访问日志数据注册成视图
    ips.createTempView("v_access_ip")

    //写SQL（Join）关联两张表数据
    val result = spark.sql("SELECT province, COUNT(*) counts FROM v_ip_rules JOIN v_access_ip ON ip>=start_num AND ip<=end_num GROUP BY province ORDER BY counts DESC")

    //触发Action
    result.show()

    //释放资源
    spark.stop()
  }
}
