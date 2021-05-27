package com.luoj.task.example.pv

import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.collection.immutable
import scala.util.Random

/**
 * @descr 自定义source
 * @date 2021/5/27 14:58
 */
class MySensorSource() extends SourceFunction[SensorReading]{

  //定义一个flag 用来标记数据源是否正常发出数据
  var running = true
  System.currentTimeMillis()

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数发生器
    val random: Random = new Random()
    val init: immutable.IndexedSeq[(String, Long, Double)] = (1 to 10).map(i=>("Sensor"+i,System.currentTimeMillis(),random.nextDouble()))
    while (running) {
      Thread.sleep(1000)
      //在上次数据基础上微调，更新温度值
      val current: immutable.IndexedSeq[(String, Long, Double)] = init.map(data=>(data._1,System.currentTimeMillis(),data._3+random.nextGaussian()))
      val readings: immutable.IndexedSeq[SensorReading] = current.map(data=>SensorReading(data._1,data._2,data._3))
      readings.map(data=>sourceContext.collect(data))
      // readings.foreach(data=>sourceContext.collect(data))
    }
  }

  override def cancel(): Unit = {
    running = false
  }

}