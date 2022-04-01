package com.bigdata.feature.table

import java.math.BigDecimal
import java.time.ZoneId

import org.apache.flink.table.api._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

import scala.annotation.varargs

object FlinkTableApiExample3 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 开启事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(600, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))
    // 状态后端-HashMapStateBackend
    env.setStateBackend(new HashMapStateBackend)
    //等价于MemoryStateBackend
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.15-learning\\checkpoint")
    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

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

    ////////////  测试数据显示
    //+I[4ea6bf31494643a6cccc32579a88f3040b2bf424ffb0f8e09c8c2f0e9098144abeadd10b6aefc7e367d0d4411d3f40591bf9, 1038502074, -1460631226, 2022-04-01T15:20:14.580Z, 2022-04-01T15:20:14.580Z]
//    val generatedTable: Table = tableEnv.from("GeneratedTable")
//    val queryTable = tableEnv.sqlQuery("select * from GeneratedTable")
//    val resultStream:DataStream[Row] = tableEnv.toChangelogStream(queryTable)
//    resultStream.print()

    ////////////////////////////////  UDF函数测试  //////////////////////////////////////////////////
    // 注册函数
    tableEnv.createTemporarySystemFunction("SubstringFunction", classOf[SubstringFunction])
//    // 在 Table API 里调用注册好的函数
////    tableEnv.from("GeneratedTable").select(call("SubstringFunction", $"a", 5, 12)).execute().print()
//    // 在 SQL 里调用注册好的函数
    tableEnv.sqlQuery("SELECT SubstringFunction(a, 5, 12) FROM GeneratedTable").execute().print()


    env.execute("FlinkTableApiExample3")

    // define function logic
    class SubstringFunction extends ScalarFunction {
      def eval(s: String, begin: Integer, end: Integer): String = {
        s.substring(begin, end)
      }
    }

    // 定义可参数化的函数逻辑
    class SubstringFunction2(endInclusive: Boolean) extends ScalarFunction {
      def eval(s: String, begin: Integer, end: Integer): String = {
        s.substring(begin, end)
      }
    }

    // 有多个重载求值方法的函数
    class SumFunction extends ScalarFunction {

      def eval(a: Integer, b: Integer): Integer = {
        a + b
      }

      def eval(a: String, b: String): Integer = {
        Integer.valueOf(a) + Integer.valueOf(b)
      }

      @varargs // generate var-args like Java
      def eval(d: Double*): Integer = {
        d.sum.toInt
      }
    }

    // function with overloaded evaluation methods
    class OverloadedFunction extends ScalarFunction {

      // no hint required
      def eval(a: Long, b: Long): Long = {
        a + b
      }

      // 定义 decimal 的精度和小数位
      @DataTypeHint("DECIMAL(12, 3)")
      def eval(a: Double, b: Double): BigDecimal = {
        BigDecimal.valueOf(a + b)
      }

      // 定义嵌套数据类型
      @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
      def eval(i: Int): Row = {
        Row.of(java.lang.String.valueOf(i), java.time.Instant.ofEpochSecond(i))
      }

    }

  }


}
