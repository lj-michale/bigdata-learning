package com.bigdata.task.example

import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableEnvironment


/**
 * @author lj.michale
 * @description
 * @date 2021-08-02
 */
object JavaTableApp {

  def main(args: Array[String]): Unit = {

    val bbSettings = EnvironmentSettings.newInstance.useBlinkPlanner.build
    val bsTableEnv = TableEnvironment.create(bbSettings)

    val sourceDDL =
      """
        |CREATE TABLE datagen (
        | f_random INT,
        | f_random_str STRING,
        | ts AS localtimestamp,
        | WATERMARK FOR ts AS ts
        |) WITH (
        | 'connector' = 'datagen',
        | 'rows-per-second'='10',
        | 'fields.f_random.min'='1',
        | 'fields.f_random.max'='5',
        | 'fields.f_random_str.length'='10'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(sourceDDL)

    val sinkDDL =
      """
        |CREATE TABLE print_table (
        | f_random int,
        | c_val bigint,
        | wStart TIMESTAMP(3)
        |) WITH ('connector' = 'print')
        |""".stripMargin

    val queryDDL = "select f_random, f_random_str, ts from datagen"

    bsTableEnv.executeSql(queryDDL).print()
    bsTableEnv.executeSql(sinkDDL)

  }
}