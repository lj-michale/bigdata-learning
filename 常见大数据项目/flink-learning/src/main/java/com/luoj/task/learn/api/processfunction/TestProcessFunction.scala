package com.luoj.task.learn.api.processfunction

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.table.api._

object TestProcessFunction {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.streaming.api.scala._
    stream.map(data => {
      val arr = data.split(",")
      Obj1(arr(0).toInt, arr(1), arr(2))
    }).process(new CustomProcessFunction).print()

  }


  class CustomProcessFunction extends ProcessFunction[Obj1, String] {

    override def processElement(value: Obj1, ctx: ProcessFunction[Obj1, String]#Context, out: Collector[String]): Unit = {
      if(value.id > 10){
        out.collect(s"${value.name},${ctx.timerService().currentProcessingTime()}")
      }
    }

  }


  case class Obj1(id:Int, name:String, str3:String)

}
