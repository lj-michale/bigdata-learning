package com.bigdata.feature.table.batch

import org.apache.flink.table.api.DataTypes.{ROW, FIELD, BIGINT, STRING, INT}
import org.apache.flink.table.api.{$, EnvironmentSettings, TableEnvironment, row}
import org.apache.flink.table.api.{long2Literal, string2Literal, int2Literal, AnyWithOperations}

/**
 * @descri Table-Api操作
 *
 * @author lj.michale
 * @date 2022-04-28
 */
object BatchTableTest {

  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings
      .newInstance()
      .inBatchMode()
      .build()

    val tEnv = TableEnvironment.create(settings)

    // 定义数据类型
    val MyOrder = ROW(
      FIELD("id", BIGINT()),
      FIELD("product", STRING()),
      FIELD("amount", INT())
    )

    val table = tEnv.fromValues(MyOrder,
      row(1L, "BMW", 1),
      row(2L, "Tesla", 8),
      row(2L, "Tesla", 8),
      row(3L, "BYD", 20)
    )

    val filtered = table.where($("amount").isGreaterOrEqual(8))

    // 调用execute，数据被collect到Job Manager
    filtered.execute().print()

  }
}
