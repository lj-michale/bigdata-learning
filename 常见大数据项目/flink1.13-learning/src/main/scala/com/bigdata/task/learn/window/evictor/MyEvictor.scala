package com.bigdata.task.learn.window.evictor

import java.lang

import com.bigdata.task.learn.window.func.WindowFuncDemo01.SensorReading
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.evictors.Evictor.EvictorContext
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

/**
 * @description Flink自定义evictor
 *              作用：用来剔除窗口中的数据。
 * @author lj.michale
 * @date 2021-07-16
 */
class MyEvictor extends Evictor[SensorReading, TimeWindow] {

  override def evictBefore(elements: lang.Iterable[TimestampedValue[SensorReading]], size: Int,
                           window: TimeWindow, evictorContext: EvictorContext): Unit = {
    val ite = elements.iterator
    // 清除掉温度>4的数据
    while (ite.hasNext) {
      if(ite.next().getValue().temperature > 40000) {
        ite.remove()
      }
    }
  }

  override def evictAfter(elements: lang.Iterable[TimestampedValue[SensorReading]], size: Int,
                          window: TimeWindow, evictorContext: EvictorContext): Unit = {

  }
}
