package com.bigdata.task.example.example001

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

object TableApiEaxmple001 {

  def main(args: Array[String]): Unit = {

//    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val runType:String = params.get("runtype")
//    println("runType: " + runType)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._
    val dataSource: DataStream[CustomerSourceBean] = env.addSource(new CustomGenerator())

    env.execute(this.getClass.getName)

  }

  // 自定义Source
  class CustomGenerator extends SourceFunction[CustomerSourceBean] {

    private var running = true

    override def run(sourceContext: SourceFunction.SourceContext[CustomerSourceBean]): Unit = {
      while (running) {
        // 产生随机CustomerSourceBean
        var randomNum: Random = new Random()
        val id:String = "JG" + randomNum.nextInt().toString + "162764612124"
        val name:String = "FLink"
        val score:Int = randomNum.nextInt()
        val customerSourceBean = CustomerSourceBean(id, name, score)
        // 利用ctx上下文将数据返回
        sourceContext.collect(customerSourceBean)
        Thread.sleep(500)
      }
    }

    override def cancel(): Unit = {
      running = false
    }

  }


  case class CustomerSourceBean(id:String, name:String, score:Int)

}
