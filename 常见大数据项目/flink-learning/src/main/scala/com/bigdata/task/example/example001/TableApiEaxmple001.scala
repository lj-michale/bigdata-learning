package com.bigdata.task.example.example001

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

import scala.util.Random

object TableApiEaxmple001 {

  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val runType:String = params.get("runtype")
//    println("runType: " + runType)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)

    import org.apache.flink.streaming.api.scala._
    val dataSource: DataStream[CustomerSourceBean] = env.addSource(new CustomGenerator())

    // 将DataStream[CustomerSourceBean] 注册成临时表
    bsTableEnv.createTemporaryView("dataSource_tmp", dataSource, $("id"), $("name"))
    val querySQL:String =
      """
        |SELECT id, name FROM dataSource_tmp
        |""".stripMargin
//    bsTableEnv.executeSql(querySQL).print()
    // 将临时实时表的query转化成TableResult
    val table:TableResult = bsTableEnv.executeSql(querySQL)
    table.print()


    // TableResult转成DataStream
//    val result: DataStream[Row] = bsTableEnv.toAppendStream[Row](table)
//    val resultDataStream:DataStream[Row] = table.toRetractStream[(String,Long)](wordCount).print()

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
