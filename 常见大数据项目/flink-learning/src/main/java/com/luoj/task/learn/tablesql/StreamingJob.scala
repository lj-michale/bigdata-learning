//package com.luoj.task.learn.tablesql
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.sources._
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.api._
//import org.apache.flink.types.Row
//import org.apache.flink.table.api.{
//  TableEnvironment,
//  TableSchema,
//  Types,
//  ValidationException
//}
//
//object StreamingJob {
//  def main(args: Array[String]) {
//    val SourceCsvPath =
//      "/<your-path-to-test-csv>/source.csv"
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    env.getConfig.disableClosureCleaner
//
//    val tEnv = StreamTableEnvironment.create(env)
//
//    val csvTableSource = CsvTableSource
//      .builder()
//      .path(SourceCsvPath)
//      .ignoreFirstLine()
//      .fieldDelimiter(",")
//      .field("name", DataTypes.STRING())
//      .field("age", DataTypes.BIGINT())
//      .field("sex", DataTypes.STRING())
//      .field("grade", DataTypes.BIGINT())
//      .field("rate", DataTypes.FLOAT())
//      .build()
//
//    tEnv.registerTableSource("source", csvTableSource)
//
//    val create_sql =
//      s"""
//         | CREATE TABLE sink_table (
//         |    name VARCHAR,
//         |    grade BIGINT,
//         |    rate FLOAT,
//         |    PRIMARY KEY (name) NOT ENFORCED
//         |) WITH (
//         |    'connector' = 'clickhouse',
//         |    'url' = 'clickhouse://<host>:<port>',
//         |    'table-name' = 'd_sink_table',
//         |    'sink.batch-size' = '1000',
//         |    'sink.partition-strategy' = 'hash',
//         |    'sink.partition-key' = 'name'
//         |)
//         |""".stripMargin
//
//    tEnv.executeSql(create_sql);
//
//    tEnv.executeSql(
//      "INSERT INTO sink_table SELECT name, grade, rate FROM source"
//    )
//  }
//}
