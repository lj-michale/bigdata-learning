package com.bigdata.task.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, FlatAggregateTable, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @program: demo
 * @description: 多行数据聚合输出多行数据
 * @author: yang
 * @create: 2021-01-16 18:48
 */
object tableAggFunc {
  def main(args: Array[String]): Unit = {
    //1、基于流执行环境创建table执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //2、读取文件，注册表视图
    tableEnv.connect(new FileSystem().path("E:\\java\\demo\\src\\main\\resources\\file\\data5.csv"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("ts",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("sensorTable")

    val sensorTable: Table = tableEnv.from("sensorTable")
    //table api
    val top2Temp = new Top2Temp()

//    val resultTable: Table = sensorTable.groupBy('id).flatAggregate(top2Temp('temperature) as('temp, 'rank))
//      .select('id,'temp,'rank)
//    resultTable.toRetractStream[Row].print("flat agg")

    env.execute(" table agg func")
  }
}

//定义一个类，表示表聚合函数的状态
class  Top2TempAcc{
  var highestTemp:Double = Double.MinValue
  var secondHighestTemp:Double = Double.MinValue

}

//自定义表聚合函数，提取所有温度值中最高的两个温度，输出(temp,rank)
class Top2Temp extends TableAggregateFunction[(Double,Int),Top2TempAcc]{
  //初始化函数
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

  //实现计算聚合结果的函数accumulate
  //注意：方法名称必须叫accumulate
  def accumulate(acc:Top2TempAcc,temp:Double): Unit ={
    //判断当前温度值是否比状态值大
    if(temp > acc.highestTemp){
      //如果比最高温度还高，排在第一，原来的顺到第二位
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    }else if(temp > acc.secondHighestTemp){
      //如果在最高和第二高之间，那么直接替换第二高温度
      acc.secondHighestTemp = temp
    }
  }

  //实现一个输出结果的方法，最终处理完表中所有的数据时调用
  //注意：方法名称必须叫emitValue
  def emitValue(acc:Top2TempAcc,out:Collector[(Double,Int)]): Unit ={
    out.collect((acc.highestTemp,1))
    out.collect((acc.secondHighestTemp,2))
  }


}