package com.bidata.demos.jira

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @descr 1、app日活规模:以周为固定周期，统计该周期内使用了inapp功能的APP的日活数量相加应用内提醒消息：
 *        1、app日活规模:以周为固定周期，统计该周期内使用了inapp功能的APP的日活数量相加（取每周三的日活）
 *        2、app送达加权:以周为固定周期，该周期内应用内提醒消息送达数/该周期内使用了inapp功能的MSGID的通知栏消息送达数
 *        3、app点击加权:以周为固定周期，该周期内应用内提醒消息点击数/该周期内使用了inapp功能的msgid的通知栏消息点击数，
 *        4、APP累计数量：统计截止到当天，累计使用过inapp功能的APP数量
 *        5、修复应用内提醒消息展示数据异常的问题
 *        6、修复部分周任务不自动执行，需人工执行的问题
 *
 *        厂商通道集成：
 *        1、免费应用开通厂商送达率：统计Android平台每天开通了厂商通道≥1的APP的送达数量汇总、发送数量汇总和送达率（送达数量汇总/发送数量汇总）
 * @author lj.michale
 * @date 2021/4/7 15:55
 */
object AmountOfAppsUsingInappStatistic {

  def main(args: Array[String]): Unit = {

    val iday = 20210305

    val sparkConf = new SparkConf().setAppName("AmountOfAppsUsingInappStatistic").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val pushInappTargetFactSQL:String =
      s"""
        | select app_key,msg_id,max(itime) as itime
        | from edw.android_push_target_log
        | where data_date= ${iday}
        | and msgtype in (103,104)
        | group by app_key,msg_id
        |""".stripMargin
    spark.sql(pushInappTargetFactSQL).createTempView("push_inapp_target_fact")

    val pushInappAppDaySQL:String =
      s"""
        |select count(1)
        |from(
        |   select app_key
        |   from push_inapp_target_fact
        |   group by app_key
        |)t1
        |""".stripMargin
    val pushInappAppDayDF:DataFrame = spark.sql(pushInappAppDaySQL)

    pushInappAppDayDF.show()

    /* 写入到对应的结果表 */

    spark.stop()

  }


}
