package com.bigdata.task.example.example001

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction

/**
 * word count
 * 1.create and use udf function
 * 2.create tmp table
 * 3.use table sql
 *
 * @author:sssuperMario
 * @date:2021-03-02
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    //create stream runtime environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //with blink planner setting
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    //create table environment
    val tableEnv = StreamTableEnvironment.create(env, settings)
    //register udf
    registerUDF(tableEnv)
    //input datasource
    val inputDataStream = env.socketTextStream("localhost", 8888)
    //word count
    val ds: DataStream[(String, Int)] = inputDataStream.filter(_.nonEmpty)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
    //register table
    tableEnv.createTemporaryView("wordcount_tmp", ds, $("word"), $("grand_total"))
    //execute sql and print result
    tableEnv.executeSql("select flink_udf_test(word) word,grand_total from wordcount_tmp")
      .print()
    //execute
    env.execute("WordCount")
  }

  /**
   * register udf
   *
   * @param env
   */
  def registerUDF(env: StreamTableEnvironment): Unit = {
    env.createTemporaryFunction("flink_udf_test", new TestUDF)
  }

  /**
   * udf
   */
  class TestUDF extends ScalarFunction {

    def eval(s: String): String = {
      "<" + s + ">"
    }
  }

}