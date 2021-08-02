package com.bigdata.task.learn.tablesql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.slf4j.{Logger, LoggerFactory}

object TableSQLExample0001 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tabEnv = StreamTableEnvironment.create(env, settings)

//    val source: CsvTableSource = CsvTableSource.builder()
//      .path("F:\\BigData\\data\\flinksql.csv").field("id", Int)
//      .field("id", Types.INT())
//      .field("name", Types.STRING())
//      .field("age", Types.INT())
//      .fieldDelimiter(",") //字段分隔符
//      .ignoreParseErrors() //忽略解析错误 解析错误的record会被跳过，默认报错
//      .ignoreFirstLine() //忽略第一行
//      .build()




  }

}
