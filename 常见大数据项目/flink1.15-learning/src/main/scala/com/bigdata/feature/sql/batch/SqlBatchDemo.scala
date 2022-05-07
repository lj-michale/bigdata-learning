package com.bigdata.feature.sql.batch

import org.apache.flink.table.api.DataTypes.{BIGINT, FIELD, INT, ROW, STRING}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, row}
import org.apache.flink.table.api.{long2Literal, string2Literal, int2Literal}

/**
 * @descri 使用SQL处理有界数据流例子
 *
 * @author lj.michale
 * @date 2022-04-28
 */
object SqlBatchDemo {

  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings
      .newInstance()
      .inBatchMode()
      .build()
    val tEnv = TableEnvironment.create(settings

    val MyOrder = ROW(
      FIELD("id", BIGINT()),
      FIELD("product", STRING()),
      FIELD("amount", INT())
    )

    val input = tEnv.fromValues(MyOrder,
      row(1L, "BMW", 1),
      row(2L, "Tesla", 8),
      row(2L, "Tesla", 8),
      row(3L, "BYD", 20)
    )

    tEnv.createTemporaryView("myOrder", input)
    val table = tEnv.sqlQuery("select product, sum(amount) as amount from myOrder group by product")

    table.execute().print()

  }

}
