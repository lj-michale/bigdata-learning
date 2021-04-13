package com.luoj.task.learn.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row


object TableApiByScala {

  def main(args: Array[String]): Unit = {

    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
    bsTableEnv.getConfig.getConfiguration.setString("parallelism.default", "8")

    bsTableEnv.executeSql(
      """
        |create table mysqlSourceTable (
        |  id int,
        |  name string,
        |  gender string,
        |  age int
        |) with (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://localhost:3306/jiguang?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
        | 'username' = 'root',
        | 'password' = 'abc1314520',
        | 'table-name' = 'student',
        | 'driver' = 'com.mysql.cj.jdbc.Driver',
        | 'scan.fetch-size' = '200'
        |)
    """.stripMargin)
    val student = bsTableEnv.sqlQuery("select * from mysqlSourceTable")
    bsTableEnv.toRetractStream[Row](student).print().setParallelism(1)


  }


}
