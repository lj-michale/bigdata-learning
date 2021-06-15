package com.bigdata.task.learn.flinksql

import com.luoj.task.example.pv.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author lj.michale
 * @description FlinkSQLExample001
 * @date 2021-05-20
 */
object FlinkSQLExample001 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)

    val inputstream:DataStream[String] = env.readTextFile("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\datasets\\info.txt")

    import org.apache.flink.api.scala._
    val dataStream = inputstream.map(item => {
      val dataArray: Array[String] = item.split(",")
      InfoReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

//    val dataStream: DataStream[SensorReading] = inputstream.map(item => {
//        val dataArray: Array[String] = item.split(",")
//        InfoReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
//    })

//    tableEnv.execute("fliksql-test")

  }

  case class InfoReading( id: String, timestamp: Long, temperature: Double )
  case class SensorReading(id:String, timestamp: Long, temperature: Double )

}
