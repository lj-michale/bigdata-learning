package com.bigdata.task.learn.window.watermark

import java.time.{Duration, ZoneId}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object WatermarkExample01 {

  def main(args: Array[String]): Unit = {

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

    val strategy = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1))    //延迟1秒
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp * 1000L   //指定事件时间字段
      })

    import org.apache.flink.api.scala._
    val dataStream = env.socketTextStream("localhost", 9999)
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toDouble, arr(2).toLong)
      }).assignTimestampsAndWatermarks(strategy)  //设置时间及水位线


    val dataStream2 = env.socketTextStream("localhost", 9999)
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toDouble, arr(2).toLong)
      }).assignTimestampsAndWatermarks(strategy)  //设置时间及水位线


    val resultStream = dataStream.map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滚动窗口
      .reduce((curdata, newdata) => (curdata._1, curdata._2.min(newdata._2), newdata._3))

    resultStream.print()
    env.execute("stream window")

  }

  case class SensorReading(id:String,temperature:Double,timestamp:Long)

}
