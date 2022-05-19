package com.bigdata.feature.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object FlinkSqlExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val kafkaSourceSql =
      """
        |CREATE TABLE kafkaTable (
        | json STRING
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'blue',
        |  'properties.bootstrap.servers' = 'p88-dataplat-slave1:9092,p88-dataplat-slave2:9092,p88-dataplat-slave3:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tableEnv.executeSql(kafkaSourceSql)

    tableEnv.sqlQuery("select * from kafkaTable").execute().print()

    env.execute()


  }

}
