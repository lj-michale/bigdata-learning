package com.luoj.task.example.example002

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Description: 订单支付实时监控(业务逻辑：匹配同一订单号的create行为和pay行为,分别输出支付订单和超时未支付的订单。)
 * @Author:
 * @Data:
 */
case class OrderLog(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\OrderLog.csv")
    val inputStream = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\OrderLog.csv")

    val dataStream = inputStream.map(data => {
        val arr = data.split(",")
        OrderLog(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val pattern = Pattern
      .begin[OrderLog]("create").where(_.eventType.equals("create"))
      .next("pay").where(_.eventType.equals("pay"))
      .within(Time.minutes(15))

    val outputTag = new OutputTag[OrderResult]("order-timeout")

    val resultStream = CEP
      .pattern(dataStream.keyBy(_.orderId), pattern)
      .select(outputTag, new OrderTimeoutResult, new OrderCompleteResult)

    resultStream.getSideOutput(outputTag).print("timeout")
    resultStream.print("success")

    env.execute("order timeout")
  }
}

class OrderTimeoutResult extends PatternTimeoutFunction[OrderLog,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderLog]], l: Long): OrderResult = {
    val create = map.get("create").get(0)
    OrderResult(create.orderId,"timeout:" + l)
  }
}

class OrderCompleteResult extends PatternSelectFunction[OrderLog,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderLog]]): OrderResult = {
    val create = map.get("create").get(0)
    val pay = map.get("pay").get(0)
    OrderResult(create.orderId,"create:"+ create.timestamp + "--pay:" + pay.timestamp)
  }
}