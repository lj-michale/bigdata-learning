package com.bigdata.feature.sql.batch

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object TableSQLJdbcAnalysisPipeline {

  def main(args: Array[String]): Unit = {

    // 定义Table环境
    val settings = EnvironmentSettings
      .newInstance()
      .inBatchMode()
      .build()
    val tEnv = TableEnvironment.create(settings)




  }

}
