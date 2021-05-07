//package com.luoj.task.connector.clickhouse
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.sources._
//import org.apache.flink.table.api._
//import org.apache.flink.types.Row
//import org.apache.flink.table.api.{TableEnvironment, TableSchema, Types, ValidationException}
////import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//
///**
// * @descr
// * Flink 在 1.11.0 版本对其 JDBC connector 进行了一次较大的重构：
//  * 重构之前（1.10.1 及之前版本），包名为 flink-jdbc 。
// * 重构之后（1.11.0 及之后版本），包名为 flink-connector-jdbc 。
// * @date 2021/5/7 18:07
// */
//object StreamingJob {
//  def main(args: Array[String]) {
//    val SourceCsvPath =
//      "/<your-path-to-test-csv>/source.csv"
//    val CkJdbcUrl =
//      "jdbc:clickhouse://<clickhouse-host>:<port>/<database>"
//    val CkUsername = "<your-username>"
//    val CkPassword = "<your-password>"
//    val BatchSize = 500 // 设置您的batch size
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val tEnv = StreamTableEnvironment.create(env)
//
//    val csvTableSource = CsvTableSource
//      .builder()
//      .path(SourceCsvPath)
//      .ignoreFirstLine()
//      .fieldDelimiter(",")
//      .field("name", Types.STRING)
//      .field("age", Types.LONG)
//      .field("sex", Types.STRING)
//      .field("grade", Types.LONG)
//      .field("rate", Types.FLOAT)
//      .build()
//
//    tEnv.registerTableSource("source", csvTableSource)
//
//    val resultTable = tEnv.scan("source").select("name, grade, rate")
//
//    val insertIntoCkSql =
//      """
//        |  INSERT INTO sink_table (
//        |    name, grade, rate
//        |  ) VALUES (
//        |    ?, ?, ?
//        |  )
//      """.stripMargin
//
//    //将数据写入 ClickHouse Sink
//    val sink = JDBCAppendTableSink
//      .builder()
//      .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
//      .setDBUrl(CkJdbcUrl)
//      .setUsername(CkUsername)
//      .setPassword(CkPassword)
//      .setQuery(insertIntoCkSql)
//      .setBatchSize(BatchSize)
//      .setParameterTypes(Types.STRING, Types.LONG, Types.FLOAT)
//      .build()
//
//    tEnv.registerTableSink(
//      "sink",
//      Array("name", "grade", "rate"),
//      Array(Types.STRING, Types.LONG, Types.FLOAT),
//      sink
//    )
//
//    tEnv.insertInto(resultTable, "sink")
//
//    env.execute("Flink Table API to ClickHouse Example")
//  }
//}
//
