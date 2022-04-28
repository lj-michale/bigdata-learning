package com.bigdata.feature.table

import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @descri Table-Api操作
 *
 * @author lj.michale
 * @date 2022-04-28
 */
object FlinkTableApiExample4 {

  def main(args: Array[String]): Unit = {

    // setup the environment
    val settings:EnvironmentSettings = EnvironmentSettings.newInstance.inStreamingMode.build
    val tEnv:TableEnvironment = TableEnvironment.create(settings)


  }

}
