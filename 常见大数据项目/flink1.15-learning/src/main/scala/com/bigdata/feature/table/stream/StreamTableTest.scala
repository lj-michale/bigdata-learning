package com.bigdata.feature.table.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{$, AnyWithOperations, EnvironmentSettings, ExplainDetail, TableEnvironment, string2Literal}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @descri Table-Api操作-处理无界数据流
 *
 * @author lj.michale
 * @date 2022-04-28
 */
object StreamTableTest {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode().build()

    val tEnv = StreamTableEnvironment.create(senv, bsSettings)
    // 此方式定义的tEnv不能使用fromDataStream函数
    // val tEnv = TableEnvironment.create(bsSettings)

    var dataStream: DataStream[String] =senv.addSource(new RandomWordSource())
//    dataStream.setParallelism(1).print()
    val table = tEnv.fromDataStream(dataStream).as("word")

    val filtered = table.where($("word").like("%t%"))
    val explain = filtered.explain(ExplainDetail.JSON_EXECUTION_PLAN)
    println(explain)

    tEnv.toDataStream(filtered).print("table")

    senv.execute()

  }
}


