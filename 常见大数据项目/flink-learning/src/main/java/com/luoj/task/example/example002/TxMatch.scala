package com.luoj.task.example.example002

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Description:
 * @Author:
 * @Data:
 */
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TxMatch {

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
      .connect(receiptDataStream.keyBy(_.txId))
      .process(new TxPayMatchResult())

    resultStream.print("info")
    resultStream.getSideOutput(new OutputTag[OrderLog]("unmatched-order")).print("unmatched-order")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-pay")).print("unmatched-pay")

    env.execute("tx match")

  }

}

// 双流connect，将符合条件的信息缓存在状态中，然后进行匹配输出
class TxPayMatchResult() extends CoProcessFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)] {
  lazy val payEventState: ValueState[OrderLog] = getRuntimeContext.getState(new ValueStateDescriptor[OrderLog]("pay-event", classOf[OrderLog]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-event", classOf[ReceiptEvent]))

  override def processElement1(value: OrderLog, ctx: CoProcessFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)]#Context, out: Collector[(OrderLog, ReceiptEvent)]): Unit = {
    val receiptEvent: ReceiptEvent = receiptEventState.value()
    if (receiptEvent == null) {
      payEventState.update(value)
      ctx.timerService().registerEventTimeTimer(value.timestamp*1000L + 5*1000L)
    } else {
      out.collect((value,receiptEvent))
      payEventState.clear()
      receiptEventState.clear()
    }
  }

  override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)]#Context, out: Collector[(OrderLog, ReceiptEvent)]): Unit = {
    val payEvent: OrderLog = payEventState.value()
    if (payEvent == null) {
      receiptEventState.update(value)
      ctx.timerService().registerEventTimeTimer(value.timestamp*1000L + 3*1000L)
    } else {
      out.collect((payEvent,value))
      payEventState.clear()
      receiptEventState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderLog, ReceiptEvent, (OrderLog, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderLog, ReceiptEvent)]): Unit = {
    val order = payEventState.value()
    val receipt = receiptEventState.value()

    if(order != null) {
      ctx.output(new OutputTag[OrderLog]("unmatched-order"),order)
    }else if (receipt != null) {
      ctx.output(new OutputTag[ReceiptEvent]("unmatched-pay"),receipt)
    }
  }
}