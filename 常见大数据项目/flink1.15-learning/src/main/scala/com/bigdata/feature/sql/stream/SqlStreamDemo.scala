package com.bigdata.feature.sql.stream

import com.bigdata.feature.table.stream.RandomWordSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @descri 使用SQL处理无界数据流例子
 *
 * @author lj.michale
 * @date 2022-04-28
 */
object SqlStreamDemo {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance()
      .inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(senv,bsSettings)

    val stream:DataStream[String] = senv.addSource(new RandomWordSource())
    val table = tEnv.fromDataStream(stream).as("word")

    val result = tEnv.sqlQuery("select * from " + table + " where word like '%t%'")
    tEnv.toDataStream(result).print()

    println(senv.getExecutionPlan)
    senv.execute()


  }

}