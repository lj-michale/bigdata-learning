package com.luoj.task.learn.source

import java.util.Calendar

import com.luoj.task.learn.state.example001.FlinkStateExample001.SensorReading
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.immutable
import scala.util.Random

class SensorSource extends RichSourceFunction[SensorReading]{

  var running: Boolean = true

  override def run(sContext:SourceFunction.SourceContext[SensorReading]) {
    val rand = new Random()
    val curFtemp = (1 to 100).map(
      i => ("sensor_" + i, rand.nextGaussian() * 20)
    )
    while(running){
      val mapTemp:immutable.IndexedSeq[(String,Double)] = curFtemp.map(
        t => (t._1,t._2 + (rand.nextGaussian()*10))
      )
      val curTime:Long = Calendar.getInstance().getTimeInMillis
      mapTemp.foreach(t => sContext.collect(SensorReading(t._1,curTime,t._2)))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running =false

}