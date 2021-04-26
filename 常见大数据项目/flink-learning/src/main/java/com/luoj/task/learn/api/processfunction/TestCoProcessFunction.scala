package com.luoj.task.learn.api.processfunction

import com.luoj.task.learn.api.processfunction.TestProcessFunction.Obj1
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object TestCoProcessFunction extends App {

  // 创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream1: DataStream[String] = env.socketTextStream("localhost",9999)
  val stream2: DataStream[String] = env.socketTextStream("localhost",8888)

  import org.apache.flink.streaming.api.scala._
  private val stream1Obj: DataStream[Obj1] = stream1
    .map(data => {
      val arr = data.split(",")
      Obj1(arr(0).toInt, arr(1), arr(2).toLong)
    })

  val stream2Rec: DataStream[Record] = stream2.map(data => {
    val arr = data.split(",")
    Record(arr(0), arr(1), arr(2).toInt)
  })

  stream1Obj
    .connect(stream2Rec)
    .process(new CustomCoProcessFunction)
    .print()

  env.execute()

  /**
   * 第一个流输入类型为 Obj1
   * 第二个流输入类型为 Record
   * 返回类型为 String
   */
  class CustomCoProcessFunction extends CoProcessFunction[Obj1,Record,String]{
    override def processElement1(value: Obj1, ctx: CoProcessFunction[Obj1, Record, String]#Context, out: Collector[String]): Unit = {
      out.collect(s"processElement1:${value.name},${value.getClass}")
    }

    override def processElement2(value: Record, ctx: CoProcessFunction[Obj1, Record, String]#Context, out: Collector[String]): Unit = {
      out.collect(s"processElement2:${value.name},${value.getClass}")
    }
  }

  case class Record(id:String, name:String, str3:Long)

}
