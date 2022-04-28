package com.bigdata.feature.sql

import org.apache.flink.table.api.Expressions.{$, lit, row}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @descri 聚合查询-聚合方式为迭代聚合，其中-U为更新前的值，+U为更新后的值
 *
 * @author lj.michale
 * @date 2022-04-28
 */
object TableSqlTest2 {

  def main(args: Array[String]): Unit = {

    // 定义Table环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    val table = tEnv.fromValues(
      row(10, "A"),
      row(20, "A"),
      row(100, "B"),
      row(200, "B")
    ).as("amount", "name")
    tEnv.createTemporaryView("tmp_table", table)

    val table_result = table
      .filter($("amount").isGreater(lit(0)))
      .groupBy($("name"))
      .select($("name"), $("amount").sum().as("amount"))

    table_result.execute().print()

    val sql_result = tEnv.sqlQuery("select name, sum(amount) as amount from tmp_table where amount > 0 group by name")
    sql_result.execute().print()

  }

}
