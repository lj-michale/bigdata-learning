package com.bigdata.task.learn.tableapi

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * @descr
 *
 * https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/tableapi/
 * @author lj.michale
 */
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
        event_time TIMESTAMP_LTZ(3),
        rowtime AS PROCTIME(),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
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
      .select($"a".lowerCase() as "a", $"b", $"event_time", $"rowtime")
      .window(Tumble over 1.hour on $"rowtime" as "hourlyWindow")
      .groupBy($"hourlyWindow", $"a")
      .select($"a", $"hourlyWindow".end as "hour", $"b".avg as "avgBillingAmount")

    tableEnv.createTemporaryView("t_result", result2)

    tableEnv.executeSql("select * from t_result").print()

    val table2 = tableEnv.fromValues(
      row(1, "ABC"),
      row(2L, "ABCDE")
    )

    val table3 = tableEnv.fromValues(
      DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("name", DataTypes.STRING())
      ),
      row(1, "ABC"),
      row(2L, "ABCDE")
    )

  }

}
