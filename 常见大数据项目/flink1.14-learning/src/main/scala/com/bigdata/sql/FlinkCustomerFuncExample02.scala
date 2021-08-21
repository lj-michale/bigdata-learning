package com.bigdata.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object FlinkCustomerFuncExample02 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode().build
    val bsTableEnv = StreamTableEnvironment.create(env, bSettings)

    val kafka_order_source =
      """
        |CREATE TABLE KafkaTable (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `behavior` STRING,
        |  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'mzpns',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(kafka_order_source)
    bsTableEnv.executeSql("select * from KafkaTable").print()



  }

}
