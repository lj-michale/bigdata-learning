package com.luoj.task.learn.udf

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


object UdafTest {

  def main(args: Array[String]): Unit = {

    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
    val ds:DataStream[Double] = bsEnv.addSource(new UdafSource)

    //注册自定义函数
    bsTableEnv.registerFunction("median", new MedianUdaf())
    bsTableEnv.createTemporaryView("tb_num",ds)
    // bsTableEnv.registerDataStream("tb_num", ds, 'num, 'proctime.proctime)

    // 每一分钟聚合一次结果
    val query: Table = bsTableEnv.sqlQuery(
      """
        |SELECT * FROM  tb_num
      """.stripMargin)

    // 每一分钟聚合一次结果
    val query2: Table = bsTableEnv.sqlQuery(
      """
        |SELECT
        |median(num)
        |FROM  tb_num
        |GROUP BY TUMBLE(proctime, INTERVAL '1' MINUTE)
      """.stripMargin)

    bsTableEnv.toAppendStream[Double](query).print()
    bsEnv.execute(s"${this.getClass.getSimpleName}")

  }
}
