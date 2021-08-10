package com.bigdata.task.learn.datastream

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import java.time.Instant
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.Schema
import org.apache.flink.table.connector.ChangelogMode

object ImplicitConversionsInScala {

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

//    // === EXAMPLE 1 ===
//    // interpret the stream as a retract stream
//    // create a changelog DataStream
//    val dataStream = env.fromElements(
//      Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
//      Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
//      Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", Int.box(12)),
//      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
//    )(Types.ROW(Types.STRING, Types.INT))
//    // interpret the DataStream as a Table
//    val table = tableEnv.fromChangelogStream(dataStream)
//    // register the table under a name and perform an aggregation
//    tableEnv.createTemporaryView("InputTable", table)
//    tableEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print()

    // === EXAMPLE 2 ===
    // interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)
    // create a changelog DataStream
    val dataStream2 = env.fromElements(
      Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
      Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
    )(Types.ROW(Types.STRING, Types.INT))
    // interpret the DataStream as a Table
    val table2 =
      tableEnv.fromChangelogStream(
        dataStream2,
        Schema.newBuilder().primaryKey("f0").build(),
        ChangelogMode.upsert())
    // register the table under a name and perform an aggregation
    tableEnv.createTemporaryView("InputTable", table2)
    tableEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print()

//    env.execute("ImplicitConversionsInScala")

  }

}
