package com.luoj.task.example.example004

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._
import org.apache.flink.connector.jdbc._
import java.sql.PreparedStatement

import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}


object StreamingJob {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)

    val ckJdbcUrl = "jdbc:clickhouse://host1:8123/tutorial"
    val ckUserName = "default"
    val ckPassword = "GhTY1OeM"
    val batchSize = 5

    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    val sensorTable = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select($"id", $"temperature")
      .filter($"id" === "sensor_1")

    val resultDataStream:DataStream[(String, Double)] = tableEnv.toAppendStream[(String, Double)](resultTable)

    val insertIntoCkSql =
      """
        |  INSERT INTO sink_table (
        |    id, temperature
        |  ) VALUES (
        |    ?, ?
        |  )
      """.stripMargin

    resultDataStream.addSink(
      JdbcSink.sink[(String, Double)](
        insertIntoCkSql,
        new CkSinkBuilder,
        new JdbcExecutionOptions.Builder().withBatchSize(batchSize).build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUrl(ckJdbcUrl)
          .withUsername(ckUserName)
          .withPassword(ckPassword)
          .build()
      )
    )
    env.execute()
  }
}


//手动实现 interface 的方式来传入相关 JDBC Statement build 函数
class CkSinkBuilder extends JdbcStatementBuilder[(String, Double)] {
  def accept(ps: PreparedStatement, v: (String, Double)): Unit = {
    ps.setString(1, v._1)
    ps.setDouble(2, v._2)
  }
}