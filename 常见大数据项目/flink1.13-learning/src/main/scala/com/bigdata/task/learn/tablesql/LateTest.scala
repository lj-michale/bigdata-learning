package com.bigdata.task.learn.tablesql

import java.sql.Timestamp
import java.time.Duration
import java.util.Calendar

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector

import scala.collection.immutable
import scala.util.Random

/**
 * @description 直接使用算子指定迟到数据输出到测输出流
 *
 * @author lj.michale
 * @date 2021-07-16
 */
object LateTest {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")
    env.setStateBackend(new EmbeddedRocksDBStateBackend)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 获取Source
    import org.apache.flink.api.scala._
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    // WatermarkStrategy
    val watermarkStrategy: WatermarkStrategy[SensorReading] = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(1)) //延迟1秒
        .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
         override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp * 1000L   //指定事件时间字段
    })

    stream.keyBy(a => (a.id)).print()

    // 侧流输出
    stream.getSideOutput(new OutputTag[String]("freezing-alarms")).print()


    env.execute("DataStream LateTest")

  }

  // 遥感数据样例类
  case  class SensorReading(id: String, timestamp: Long, temperature: Double)




  // 为什么用`ProcessFunction`? 因为没有keyBy分流
  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
    // 定义侧输出标签
    lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

    // 来一条数据，调用一次
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        // 将报警信息发送到侧输出流
        ctx.output(freezingAlarmOutput, s"传感器ID为 ${value.id} 的传感器发出低于32华氏度的低温报警！")
      }
      out.collect(value) // 在主流上，将数据继续向下发送
    }
  }


  class MyKeyedProcessByProccessTime extends KeyedProcessFunction[String, (String, String), String] {
    // 来一条数据调用一次
    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, out: Collector[String]): Unit = {
      // 当前机器时间
      val curTime = ctx.timerService().currentProcessingTime()
      // 当前机器时间10s之后，触发定时器
      ctx.timerService().registerProcessingTimeTimer(curTime + 10 * 1000L)
    }
    // 基于处理时间的定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("位于时间戳：" + new Timestamp(timestamp) + "的定时器触发了！")
    }
  }

  class MyKeyedProcessByEventTime extends KeyedProcessFunction[String, (String, Long), String] {
    // 来一条数据调用一次
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      // 在当前元素时间戳的10s钟以后，注册一个定时器，定时器的业务逻辑由`onTimer`函数实现
      ctx.timerService().registerEventTimeTimer(value._2 + 10 * 1000L)
      out.collect("当前水位线是：" + ctx.timerService().currentWatermark())
    }
    // 基于事件时间的定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("位于时间戳：" + timestamp + "的定时器触发了！")
    }
  }

  // 编造自定义Source
  class SensorSource extends RichSourceFunction[SensorReading]{
    //表示数据源是否运行正常
    var running: Boolean = true
    //上下文参数来发送数据
    override def run(sContext:SourceFunction.SourceContext[SensorReading]) {
      val rand = new Random()
      //使用高斯噪声产生随机温度
      val curFtemp = (1 to 10).map(
        i => ("sensor_" + i, rand.nextGaussian() * 20)
      )
      //产生无限流数据
      while(running){
        val mapTemp:immutable.IndexedSeq[(String,Double)] = curFtemp.map(
          t => (t._1,t._2 + (rand.nextGaussian()*10))
        )
        //产生时间戳
        val curTime:Long = Calendar.getInstance().getTimeInMillis
        //发送出去
        mapTemp.foreach(t => sContext.collect(SensorReading(t._1,curTime,t._2)))
        //每隔100ms发送一条传感器数据
        Thread.sleep(100)
      }
    }
    override def cancel(): Unit = running =false
  }

}
