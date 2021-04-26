package com.luoj.task.learn.api.processfunction

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 全量聚合函数
 */
object TestProcessFunctionByWindow {

  // 每隔5秒统计每个基站的日志数量
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // source
    var stream = env.socketTextStream("flink101", 8888)
      .map(line => {
        var arr = line.split(",")
        Log(arr(0).trim,arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    // 设置并行度
    stream.setParallelism(1)
    stream.map(log=> (log.sid, 1))
      .keyBy(_._1)
      //        .timeWindow(Time.seconds(5))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new MyProcessWindowFunction)  // 一个窗口结束的时候调用一次（在一个并行度中）
      .print()


    env.execute("TestReduceFunctionByWindow")
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), (String, Long), String, TimeWindow] {

  // 一个窗口结束的时候调用一次（一个分组执行一次），不适合大量数据，全量数据保存在内存中，会造成内存溢出
  override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Long)]): Unit = {
    // 聚合，注意:整个窗口的数据保存到Iterable，里面有很多行数据, Iterable的size就是日志的总行数
    out.collect(key, elements.size)
  }
}

case class Log(sid:String,var callOut:String, var callIn:String, callType:String, callTime:Long, duration:Long)

