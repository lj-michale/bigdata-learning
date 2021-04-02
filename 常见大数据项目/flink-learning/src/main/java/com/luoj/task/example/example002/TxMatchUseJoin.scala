package com.luoj.task.example.example002

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Description:
 * @Author:
 * @Data:
 */
object TxMatchUseJoin {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderResource = getClass.getResource("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\OrderLog.csv")
    val orderInputStream = env.readTextFile(orderResource.getPath)
    val orderDataStream = orderInputStream.map(data => {
        val arr = data.split(",")
        OrderLog(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    val receiptResource = getClass.getResource("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\ReceiptLog.csv")
    val ReceiptInputStream = env.readTextFile(receiptResource.getPath)
    val receiptDataStream = ReceiptInputStream.map(data => {
        val arr = data.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000L)


    val resultStream = orderDataStream.filter(_.eventType.equals("pay")).keyBy(_.txId)
      .intervalJoin(receiptDataStream.keyBy(_.txId))
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new TxMatchWithJoinResult)

    resultStream.print("success")
    env.execute("tx match join")

  }

}

class TxMatchWithJoinResult extends ProcessJoinFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)] {
  override def processElement(left: OrderLog, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)]#Context, out: Collector[(OrderLog, ReceiptEvent)]): Unit = {
    out.collect((left,right))
  }
}