package com.bigdata.window.evictor

import java.lang

import com.bigdata.bean.RawData
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.evictors.Evictor.EvictorContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

class MyEvictor extends Evictor[RawData, TimeWindow] {

  override def evictBefore(elements: lang.Iterable[TimestampedValue[RawData]], size: Int,
                           window: TimeWindow, evictorContext: EvictorContext): Unit = {
    val ite = elements.iterator
    // 清除掉buyMoney<0的数据
    while (ite.hasNext) {
      if(ite.next().getValue().buyMoney < 0) {
        ite.remove()
      }
    }
  }

  override def evictAfter(elements: lang.Iterable[TimestampedValue[RawData]], size: Int,
                          window: TimeWindow, evictorContext: EvictorContext): Unit = {

  }
}
