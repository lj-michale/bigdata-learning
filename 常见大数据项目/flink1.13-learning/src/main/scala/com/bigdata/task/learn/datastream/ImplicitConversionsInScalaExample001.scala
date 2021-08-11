package com.bigdata.task.learn.datastream

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row


object ImplicitConversionsInScalaExample001 {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    // create environments of both APIs
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    val dataStream: DataStream[(Int, String)] = env.fromElements((42, "hello"))

//    // call toChangelogTable() implicitly on the DataStream object
//    val table: Table = dataStream.toChangelogTable(tableEnv)
//
//    // force implicit conversion
//    val dataStreamAgain1: DataStream[Row] = table
//
//    // call toChangelogStream() implicitly on the Table object
//    val dataStreamAgain2: DataStream[Row] = table.toChangelogStream

    env.execute(s"${this.getClass.getName}")

  }

}
