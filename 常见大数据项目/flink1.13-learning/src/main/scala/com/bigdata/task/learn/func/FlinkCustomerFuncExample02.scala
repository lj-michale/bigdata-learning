package com.bigdata.task.learn.func

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object FlinkCustomerFuncExample02 {

  def main(args: Array[String]): Unit = {

//    val settings = EnvironmentSettings
//      .newInstance()
//      .inStreamingMode()
//      .build()
//    val tableEnv = TableEnvironment.create(settings)

    val settings = EnvironmentSettings.newInstance().build()
    val bsTableEnv = TableEnvironment.create(settings)

    val kafka_order_source =
      """
        |CREATE TABLE json_source (
        |    id            BIGINT,
        |    name          STRING,
        |    `date`        DATE,
        |    obj           ROW<time1 TIME,str STRING,lg BIGINT>,
        |    arr           ARRAY<ROW<f1 STRING,f2 INT>>,
        |    `time`        TIME,
        |    `timestamp`   TIMESTAMP(3),
        |    `map`         MAP<STRING,BIGINT>,
        |    mapinmap      MAP<STRING,MAP<STRING,INT>>,
        |    proctime as PROCTIME()
        | ) WITH (
        |    'connector' = 'kafka-0.11',
        |    'topic' = 'mzpns',
        |    'properties.bootstrap.servers' = 'localhost:9092',
        |    'properties.group.id' = 'testGroup',
        |    'format' = 'json',
        |    'scan.startup.mode' = 'latest-offset'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(kafka_order_source)
    bsTableEnv.executeSql("select * from json_source").print()



  }

}
