package com.bigdata.task.learn.datastream

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * @description Converting between DataStream and Table
 *              Examples for createTemporaryView
 *  https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/data_stream_api/
 * @author lj.michale
 * @date 2021-07-16
 */
object DataStreamAPIIntegrationExample004 {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    // create environments of both APIs
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
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
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    // create some DataStream
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(Long, String)] = env.fromElements(
      (12L, "Alice"),
      (0L, "Bob"))

    // === EXAMPLE 1 ===
    // register the DataStream as view "MyView" in the current session
    // all columns are derived automatically
    tableEnv.createTemporaryView("MyView", dataStream)
    tableEnv.from("MyView").printSchema()
    // prints:
    // (
    //  `_1` BIGINT NOT NULL,
    //  `_2` STRING
    // )

    // === EXAMPLE 2 ===
    // register the DataStream as view "MyView" in the current session,
    // provide a schema to adjust the columns similar to `fromDataStream`
    // in this example, the derived NOT NULL information has been removed
    tableEnv.createTemporaryView(
      "MyView",
      dataStream,
      Schema.newBuilder()
        .column("_1", "BIGINT")
        .column("_2", "STRING")
        .build())
    tableEnv.from("MyView").printSchema()
    // prints:
    // (
    //  `_1` BIGINT,
    //  `_2` STRING
    // )

    // === EXAMPLE 3 ===
    // use the Table API before creating the view if it is only about renaming columns
    tableEnv.createTemporaryView("MyView", tableEnv.fromDataStream(dataStream).as("id", "name"))
    tableEnv.from("MyView").printSchema()
    // prints:
    // (
    //  `id` BIGINT NOT NULL,
    //  `name` STRING
    // )


  }

}
