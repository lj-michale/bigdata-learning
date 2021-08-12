package com.bigdata.task.learn.window.func

import java.time.ZoneId
import java.util.Calendar

import com.bigdata.task.learn.flinksql.FlinkSQLExample0006.SensorSource
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.immutable
import scala.util.Random
//import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

/**
 * @description Flink窗口函数计算使用例子
 *
 * @author lj.michale
 * @date 2021-07-16
 */
object WindowFuncDemo01 {

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

    import org.apache.flink.api.scala._
    val sensorData: DataStream[SensorReading] = env.addSource(new SensorSource)

    val outputTag = new OutputTag[SensorReading]("late-data") {}

    // 计算5s滚动窗口中的最低和最高的温度。输出的元素包含了(流的Key, 最低温度, 最高温度, 窗口结束时间)
    val minMaxTempPerWindow: DataStream[MinMaxTemp] = sensorData
      //进行分组
      .keyBy(_.id)
      //进行窗口计算（基于处理时间ProcessingTime）
      .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(15)))
      //设置数据延迟容忍时间
      .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(5))
      // 延迟数据旁路输出
      .sideOutputLateData(outputTag)
      //使用窗口函数进行对窗口数据进行计算
      .process(new HighAndLowTempProcessFunction)

    minMaxTempPerWindow.print()

    env.execute(s"${this.getClass.getName}")

  }

  case class SensorReading(id:String,timestamp:Long,temperature:Double)
  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  // 定义窗口处理函数
  class HighAndLowTempProcessFunction
    extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                         ctx: Context,
                         vals: Iterable[SensorReading],
                         out: Collector[MinMaxTemp]): Unit = {
      val temps = vals.map(_.temperature)
      val windowEnd = ctx.window.getEnd
      out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
    }
  }

  // 自定义数据源
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
