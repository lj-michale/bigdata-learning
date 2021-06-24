package com.luoj.task.learn.state.example001

import java.util.Calendar

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.util.Collector

import scala.collection.immutable
import scala.util.Random

/**
 * @descr Flink状态编程：state
 *
 * @author lj.michale
 * @date 2021/6/23 10:24
 */
object FlinkStateExample001 {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, org.apache.flink.api.common.time.Time.seconds(10), org.apache.flink.api.common.time.Time.seconds(1)))
    env.getConfig.setAutoWatermarkInterval(5000L)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    env.setParallelism(1)
    import org.apache.flink.api.scala._

    val inputStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    val keyedStream: KeyedStream[SensorReading, String] = inputStream.map(line => {
      val fildes: Array[String] = line.toString.split(",")
      val sensorReading = new SensorReading(fildes(0).trim.reverse.dropRight(14).reverse, fildes(1).trim.toLong, fildes(2).trim.dropRight(1).toDouble)
      // logger.info(">>>>>>>>>>>>>input:{} ", sensorReading)
      sensorReading
    }).keyBy(_.id)

    /////////////////////////////////// 一般有三种使用状态的方法 /////////////////////////////////////
    // 这里实现状态编程，实现当连续两次温度相差10度时进行报警业务逻辑
    // 方法I: 通过process实现
//    val processed: DataStream[(String, Double, Double)] = keyedStream.process(new StateProcess(10.0))
//    processed.print(">>>>>>>>>>>>> 连续两次温度相差10度").setParallelism(1)

    // 方法II: 通过富函数实现
    val flatMaped: DataStream[(String, Double, Double)] = keyedStream.flatMap(new StateFunction(10.0))
    flatMaped.print(">>>>>>>>>>>>> 连续两次温度相差10度, 通过方法II进行状态计算").setParallelism(1)

    // 方法III: 调用KeyedStream的flatMapWithState方法来实现
    // 通过带状态的flatMap实现, [(String, Double, Double),Double] 输出类型， 状态类型.
    // 并且需要在参数中传递一个方法
    val flatMapWithStated: DataStream[(String, Double, Double)] = keyedStream.flatMapWithState[(String, Double, Double), Double]({
      // 如果没有状态，也就是没有数据来过，则将当前的数据保存为状态值
      case (input: SensorReading, None) => {
        (List.empty, Some(input.timepreture))
      }
      // 如果有状态，则和当前数据进行比较，并将当前数据保存为状态值
      case (input: SensorReading, temp: Some[Double]) => {
        val diff: Double = (input.timepreture - temp.get).abs
        if (diff > 10.0) {
          (List((input.id, temp.get, input.timepreture)), Some(input.timepreture))
        } else {
          (List.empty, Some(input.timepreture))
        }
      }
    })
    flatMapWithStated.print(">>>>>>>>>>>>> 连续两次温度相差10度, 通过方法III进行状态计算").setParallelism(1)


    env.execute(this.getClass.getName)

  }

  case class SensorReading( id: String, timestamp: Long, timepreture: Double)

  class SensorSource extends RichSourceFunction[SensorReading]{
    //表示数据源是否运行正常
    var running: Boolean = true
    //上下文参数来发送数据
    override def run(sContext:SourceFunction.SourceContext[SensorReading]) {
      val rand = new Random()
      //使用高斯噪声产生随机温度
      val curFtemp = (1 to 100).map(
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


  /**
   * 温度跳变阈值输出报警信息
   * @param temp 温度差阈值
   */
  class  StateProcess(temp: Double) extends  KeyedProcessFunction[String, SensorReading, (String, Double, Double)]{

    //  定义一个状态变量，用于报错上次的温度值
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState[Double](new
        ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

    override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading,
      (String, Double, Double)]#Context, collector: Collector[(String, Double, Double)]): Unit = {

      val lastTemp: Double = lastTempState.value()

      val dff = (lastTemp - i.timepreture).abs

      if(dff > temp){
        collector.collect((i.id, lastTemp, i.timepreture))
      }

      // 更改温度状态值
      lastTempState.update(i.timepreture)
    }
  }

  class StateFunction(temp: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    var lastTempState: ValueState[Double] =_

    override def open(parameters: Configuration): Unit = {
      //  定义一个状态变量，用于报错上次的温度值
      lastTempState = getRuntimeContext.getState[Double](new
          ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
    }

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      val lastTemp: Double = lastTempState.value()
      val dff = (lastTemp - value.timepreture).abs
      if(dff > temp) {
        out.collect((value.id, lastTemp, value.timepreture))
      }
      // 更改温度状态值
      lastTempState.update(value.timepreture)
    }

  }

}
