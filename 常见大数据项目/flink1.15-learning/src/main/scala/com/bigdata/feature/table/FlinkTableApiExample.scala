package com.bigdata.feature.table

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/**
 * @descri
 * @author lj.michale
 * @date 2022-04-01
 */
object FlinkTableApiExample {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 开启事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
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

    val generatedTable: Table = tableEnv.from("GeneratedTable")

    import org.apache.flink.table.api._
    val dataStream: DataStream[Row] = tableEnv.toChangelogStream(
      generatedTable,
      Schema.newBuilder()
        .column("name", "STRING")
        .column("score", "INT")
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build())

    dataStream.print()


    env.execute(" table function test job")

  }

}
