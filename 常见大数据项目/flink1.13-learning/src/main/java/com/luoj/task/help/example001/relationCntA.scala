package com.luoj.task.help.example001

import java.io.{InputStream, OutputStream}

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousEventTimeTrigger, ContinuousProcessingTimeTrigger, ProcessingTimeTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @descr 帮人调试code
 * @date 2021/7/1 16:28
 */
object relationCntA {

  def main(args: Array[String]): Unit = {

    val windowTime = TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))
    val triggerInterval = 40
    var backendFilePath = ""
    val parallelism = 1
    val evictorTime = 40
    backendFilePath = "file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint" //存储checkpoint数据,//fs状态后端配置,如为file:///,则在taskmanager的本地
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val endStream=env.addSource(new CustomerSource)

    //先做条件过滤

    val outStream = endStream
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Tuple2[Long,Long]] {
        var currentMaxTimestamp = 0L
        val maxOutOfOrderness = 2000L //2秒
        var lastEmittedWatermark: Long = Long.MinValue

        override def extractTimestamp(t: Tuple2[Long,Long], l: Long): Long = {
          val timestamp = t._1
          println("---------2---timestamp--------" + timestamp)
          if (timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp
          }
          timestamp
        }

        override def getCurrentWatermark: Watermark = {
          //允许延迟2秒
          val potentialWM = currentMaxTimestamp - maxOutOfOrderness
          if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM
          }
          new Watermark(lastEmittedWatermark)
        }

      })
      .keyBy(data => data._2)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
      .trigger(ProcessingTimeTrigger.create)
      .process(new MyProcessWindowFunction).setParallelism(1).print()

//    val outStream = endStream
//      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Tuple2[Long,Long]] {
//        var currentMaxTimestamp = 0L
//        val maxOutOfOrderness = 2000L //2秒
//        var lastEmittedWatermark: Long = Long.MinValue
//
//        override def extractTimestamp(t: Tuple2[Long,Long], l: Long): Long = {
//          val timestamp = t._1
//          println("---------2---timestamp--------" + timestamp)
//          if (timestamp > currentMaxTimestamp) {
//            currentMaxTimestamp = timestamp
//          }
//          timestamp
//        }
//
//        override def getCurrentWatermark: Watermark = {
//          //允许延迟2秒
//          val potentialWM = currentMaxTimestamp - maxOutOfOrderness
//          if (potentialWM >= lastEmittedWatermark) {
//            lastEmittedWatermark = potentialWM
//          }
//          new Watermark(lastEmittedWatermark)
//        }
//
//      })
//      .keyBy(data => data._2)
//      .window(TumblingEventTimeWindows.of(Time.days(1), Time.minutes(1))) //统计今天内的数据量
//      .trigger(ContinuousEventTimeTrigger.of(Time.seconds(1)))
//      //.evictor(TimeEvictor.of(Time.seconds(evictorTime), true))
//      .process(new MyProcessWindowFunction)

    env.execute("kafka test")

  }

  class MyProcessWindowFunction extends ProcessWindowFunction[(Long, Long), (String, Long), Long, TimeWindow] {
    // 一个窗口结束的时候调用一次（一个分组执行一次），不适合大量数据，全量数据保存在内存中，会造成内存溢出
    override def process(key: Long, context: Context, elements: Iterable[(Long, Long)], out: Collector[(String, Long)]): Unit = {
      // 聚合，注意:整个窗口的数据保存到Iterable，里面有很多行数据, Iterable的size就是日志的总行数
      println("dddddddddddddddddd")
    }
  }

}


