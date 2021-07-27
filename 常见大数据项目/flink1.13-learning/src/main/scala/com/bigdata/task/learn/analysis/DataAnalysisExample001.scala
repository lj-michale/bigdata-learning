package com.bigdata.task.learn.analysis

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author lj.michale
 * @description FlinkSQLExample001
 * @date 2021-05-20
 */
object DataAnalysisExample001 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val mysqlSourceSql =
      """
        |create table mysqlSourceTable (
        |  user_id int,
        |  item_id int,
        |  behavior_type int,
        |  user_geohash int,
        |  item_category int,
        |  buy_time string
        |) with (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://localhost:3306/analysis?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
        | 'username' = 'root',
        | 'password' = 'abc1314520',
        | 'table-name' = 'tianchi_mobile_recommend_train_user',
        | 'driver' = 'com.mysql.cj.jdbc.Driver',
        | 'scan.fetch-size' = '200'
        |)
    """.stripMargin

    tEnv.executeSql(mysqlSourceSql)
    tEnv.executeSql("select * from mysqlSourceTable").print()

    env.execute("DataAnalysisExample001")

  }

}
