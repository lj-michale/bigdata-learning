package com.bigdata.examples

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object DataStreamAPIIntegration001 {

  def main(args: Array[String]): Unit = {

    // create environments of both APIs
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    // create a DataStream
    val dataStream = env.fromElements("Alice", "Bob", "John")

    // interpret the insert-only DataStream as a Table
    val inputTable = tableEnv.fromDataStream(dataStream)

    // register the Table object as a view and query it
    tableEnv.createTemporaryView("InputTable", inputTable)
    val resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable")

    // interpret the insert-only Table as a DataStream again
    val resultStream = tableEnv.toDataStream(resultTable)

    // add a printing sink and execute in DataStream API
    resultStream.print()
    env.execute()


  }

}
