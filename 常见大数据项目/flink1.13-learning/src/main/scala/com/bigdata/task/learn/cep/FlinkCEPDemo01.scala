package com.bigdata.task.learn.cep

import java.time.ZoneId
import java.util.Calendar

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import scala.collection.immutable
import scala.util.Random


/**
 * @description Flink CEP 使用例子
 * @author lj.michale
 * @date 2021-07-16
 */
object FlinkCEPDemo01 {

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
    // 等价于MemoryStateBackend
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    import org.apache.flink.api.scala._
    val sensorData: DataStream[SensorReading] = env.addSource(new SensorSource)

//    val pattern = Pattern.begin[SensorReading]("start").where(_.temperature == 42)
//      .next("middle").subtype(classOf[MinMaxTemp]).where(_.max >= 10.0)
//      .followedBy("end").where(_.temperature == "end")
//
//    val patternStream = CEP.pattern(sensorData, pattern)


    env.execute(s"${this.getClass.getName}")

  }

  case class SensorReading(id:String,timestamp:Long,temperature:Double)
  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  class SensorSource extends RichSourceFunction[SensorReading] {
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
