package com.luoj.task.example.example002

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * @Description: 使用FLinkSQL实现需求：每五秒钟输出最近一个小时的topN点击商品
 * @Author:
 * @Data:
 */
object HotItemsWithSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\UserBehavior.csv")

    // 包装成样例类，指定水位线事件时间
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000)

    // 定义表执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 使用TableAPI进行聚合操作
//    val dataTable: Table = tableEnv.fromDataStream(dataStream,$"itemId",$"behavior",$"timestamp".rowtime as "ts")
//    val aggTable = dataTable
//      .filter($"behavior" === "pv")
//      .window(Slide over 1.hours every 5.minutes on $"ts" as "sw")
//      .groupBy($"itemId", $"sw")
//      .select($"itemId", $"sw".end as "windowEnd", $"itemId".count as "cnt")

    // 使用TableSQL进行聚合操作
    tableEnv.createTemporaryView("dataTable",dataStream,$"itemId",$"behavior",$"timestamp".rowtime as "ts")
    val aggTable = tableEnv.sqlQuery(
      """
        |SELECT
        |  itemId,
        |  HOP_END(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR) as windowEnd,
        |  count(itemId) as cnt
        |FROM dataTable
        |WHERE behavior='pv'
        |GROUP BY
        |  itemId,
        |  HOP(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR)
        |""".stripMargin)

    // 使用SQL实现TopN的选取
    tableEnv.createTemporaryView("aggTable",aggTable,$"itemId",$"windowEnd",$"cnt")
    val resultTable = tableEnv.sqlQuery(
      """
        |SELECT
        |  *
        |FROM (
        |  SELECT
        |    *,
        |    row_number() OVER(PARTITION BY windowEnd ORDER BY cnt desc) AS row_num
        |  FROM aggTable
        |) tmp
        |WHERE row_num <=5
        |""".stripMargin)
    resultTable.toRetractStream[Row].print()

    env.execute("hot items table sql")
  }
}
