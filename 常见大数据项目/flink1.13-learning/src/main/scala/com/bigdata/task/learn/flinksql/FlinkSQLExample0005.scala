package com.bigdata.task.learn.flinksql

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object FlinkSQLExample0005 {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))
    // 状态后端-HashMapStateBackend
    env.setStateBackend(new HashMapStateBackend)
    //等价于MemoryStateBackend
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    // 在创建表的 DDL 中定义
    tableEnv.executeSql(
      """
      CREATE TABLE user_actions (
          user_name STRING,
          data STRING,
          user_action_time AS PROCTIME() -- 声明一个额外的列作为处理时间属性
      )
      WITH ('connector'='datagen')
      """
    )

    val table:Table = tableEnv.from("user_actions")

    val table2:Table = tableEnv.sqlQuery(
      """
        |SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
        |FROM user_actions
        |GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE)
        |""".stripMargin)

    val dataStream: DataStream[Row] = tableEnv.toChangelogStream(table)
//    dataStream.print()

    val dataStream2: DataStream[Row] = tableEnv.toChangelogStream(table2)
    dataStream2.print()

    env.execute("FlinkSQLExample0005")

  }

}
