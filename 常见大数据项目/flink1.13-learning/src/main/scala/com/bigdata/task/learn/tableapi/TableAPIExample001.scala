package com.bigdata.task.learn.tableapi

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object TableAPIExample001 {

  def main(args: Array[String]): Unit = {

    // 环境配置
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tableEnv = TableEnvironment.create(settings)

    tableEnv.executeSql(
      """
      CREATE TABLE GeneratedTable (
        a STRING,
        b INT,
        c INT,
        rowtime TIMESTAMP_LTZ(3)
      )
      WITH ('connector'='datagen')
      """
    )

    val orders = tableEnv.from("GeneratedTable")
//    val result = orders.groupBy($"a")
//                .select($"a", $"b".count as "cnt")
//                .execute()
//                .print()

    val result2: Table = orders
      .filter($"a".isNotNull && $"b".isNotNull && $"c".isNotNull)
      .select($"a".lowerCase() as "a", $"b", $"rowtime")
      .window(Tumble over 1.hour on $"rowtime" as "hourlyWindow")
      .groupBy($"hourlyWindow", $"a")
      .select($"a", $"hourlyWindow".end as "hour", $"b".avg as "avgBillingAmount")

    tableEnv.createTemporaryView("t_result", result2)

    tableEnv.executeSql("select * from t_result").print()

  }

}
