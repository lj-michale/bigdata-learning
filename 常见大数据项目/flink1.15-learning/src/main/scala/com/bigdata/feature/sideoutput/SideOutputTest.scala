package com.bigdata.feature.sideoutput

import com.bigdata.bean.DataSchema.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @descri 用侧输出流实现一个分流操作
 *
 * @author lj.michale
 * @date 2022-04-01
 */
object SideOutputTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val dataDstream: DataStream[SensorReading] = inputDStream
      .map( data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultDStrem: DataStream[SensorReading] = dataDstream
      .process( SideOutputTest(30.0) )

    dataDstream.print("data")
    // 主流为高于30.0度
    resultDStrem.print( "high" )
    // 侧输出流需要进行获取
    resultDStrem.getSideOutput( new OutputTag[SensorReading]("low-temp") ).print("low")

    env.execute("SideOutput test job")
  }

}

/**
 * 自定义一个方法，判断温度是否大于30度，小于30度输出到测输出流，大于30主流
 * @param tpr
 */
case class SideOutputTest(tpr: Double) extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if( value.temperature >= tpr ) {
      out.collect( value )
    } else {
      ctx.output( new OutputTag[SensorReading]("low-temp"), value)
    }
  }

}