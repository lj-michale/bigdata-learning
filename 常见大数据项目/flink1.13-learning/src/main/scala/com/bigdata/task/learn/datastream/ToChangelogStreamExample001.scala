package com.bigdata.task.learn.datastream

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import java.time.Instant

/**
 * @description Converting between DataStream and Table
 *              Examples for toChangelogStream
 *  https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/data_stream_api/
 * @author lj.michale
 * @date 2021-07-16
 */
object ToChangelogStreamExample001 {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    // create environments of both APIs
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
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
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    // create Table with event-time
    tableEnv.executeSql(
      """
      CREATE TABLE GeneratedTable (
        name STRING,
        score INT,
        event_time TIMESTAMP_LTZ(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
      )
      WITH ('connector'='datagen')
      """
    )

    val table = tableEnv.from("GeneratedTable")

    // === EXAMPLE 1 ===
    // convert to DataStream in the simplest and most general way possible (no event-time)
    val simpleTable = tableEnv
      .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
      .as("name", "score")
      .groupBy($"name")
      .select($"name", $"score".sum())
    tableEnv
      .toChangelogStream(simpleTable)
      .executeAndCollect()
      .foreach(println)
    // prints:
    // +I[Bob, 12]
    // +I[Alice, 12]
    // -U[Alice, 12]
    // +U[Alice, 14]



    env.execute(s"${this.getClass.getName}")

  }

}
