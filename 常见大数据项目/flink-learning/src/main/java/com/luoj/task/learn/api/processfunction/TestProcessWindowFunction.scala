//package com.luoj.task.learn.api.processfunction
//
//import java.text.SimpleDateFormat
//
//import com.luoj.task.learn.api.processfunction.TestProcessFunction.Obj1
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
//object TestProcessWindowFunction extends App {
//
//  val environment:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//  val stream1: DataStream[String] = environment.socketTextStream("localhost",9999)
//
//  val stream2: DataStream[Obj1] = stream1.map(data => {
//    val arr = data.split(",")
//    Obj1(arr(0).toInt, arr(1), arr(2).toInt)
//  })
//
//  // 设置一个窗口时间是 5 秒的窗口
//  stream2
//    .keyBy(_.id)
//    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//    .process(new CuntomProcessFunction)
//    .print("TestWindowFunction")
//
//  environment.execute()
//
//}
//
///**
// *
// * IN –输入值的类型。
// * OUT –输出值的类型。
// * KEY –密钥的类型。
// * W –窗口的类型
// */
//class CuntomProcessFunction extends ProcessWindowFunction[Obj1, String, String, TimeWindow] {
//  override def process(key: String, context: Context, elements: Iterable[Obj1], out: Collector[String]): Unit = {
//    var count = 0
//    val sdf = new SimpleDateFormat("HH:mm:ss")
//
//    println(
//      s"""
//         |window key：${key},
//         |开始时间：${sdf.format(context.window.getStart)},
//         |结束时间：${sdf.format(context.window.getEnd)},
//         |maxTime：${sdf.format(context.window.maxTimestamp())}
//         |""".stripMargin)
//
//    // 遍历，获得窗口所有数据
//    for (obj <- elements) {
//      println(obj.toString)
//      count += 1
//    }
//    out.collect(s"Window ${context.window} , count : ${count}")
//  }
//
//}