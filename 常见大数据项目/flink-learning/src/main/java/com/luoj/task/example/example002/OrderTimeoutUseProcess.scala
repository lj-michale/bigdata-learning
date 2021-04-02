package com.luoj.task.example.example002

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Description:
 * @Author:
 * @Data:
 */
object OrderTimeoutUseProcess {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val resource = getClass.getResource("/OrderLog.csv")
        val inputStream = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\OrderLog.csv")
//    val inputStream = env.socketTextStream("192.168.32.242", 7777)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        OrderLog(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream = dataStream
      .keyBy(_.orderId)
      .process(new OrderProcessResult)

    resultStream.getSideOutput(new OutputTag[OrderResult]("order-timeout")).print("warning")
    resultStream.print("success")


    env.execute("order timeout")
  }
}

// 只需要匹配create和pay两条数据即可，
class OrderProcessResult extends KeyedProcessFunction[Long, OrderLog, OrderResult] {

  lazy val createState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("create-state", classOf[Boolean]))
  lazy val payState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("pay-state", classOf[Boolean]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
  val outputTag = new OutputTag[OrderResult]("order-timeout")

  override def processElement(value: OrderLog, ctx: KeyedProcessFunction[Long, OrderLog, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val isCreate = createState.value()
    val isPayed = payState.value()
    val timerTs = timerTsState.value()

    val timestamp = ctx.timestamp()
    // 1 当前到达的是订单创建记录
    if (value.eventType.equals("create")) {
      if (isPayed) {
        // 1.1 到达的订单已经支付完成，清空状态并输出支付完成记录
        ctx.timerService().deleteEventTimeTimer(timerTs)
        createState.clear()
        payState.clear()
        timerTsState.clear()
        out.collect(OrderResult(value.orderId, "pay first,order completed!"))
      } else {
        // 1.2 到达的订单未支付，需要等待支付记录达到
        val ts = value.timestamp * 1000L + 15 * 60000
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        createState.update(true)
      }
    }
    // 2 当前到达的是订单支付记录
    else if (value.eventType.equals("pay")) {
      if (isCreate) {
        // 2.1 如果已经创建订单了，则匹配成功。还要判断一下支付时间是否超过了定时器时间
        if (value.timestamp * 1000L <= timerTs) {
          // 2.1.1 如果支付时间小于定时器时间，正常输出
          ctx.timerService().deleteEventTimeTimer(timerTs)
          out.collect(OrderResult(value.orderId, "create first,order completed!"))
        } else {
          // 2.1.2 如果支付时间大于定时器时间，则超时，输出到侧输出流
          ctx.output(outputTag, OrderResult(value.orderId, "支付时间超过订单有效时间，发生异常"))
          Thread.sleep(10)
        }
        createState.clear()
        payState.clear()
        timerTsState.clear()
      } else {
        // 2.2 如果订单未创建，那么可能是无序数据造成的，设置定时器触发时间为支付时间，等待水位线漫过当前支付时间时触发
        val ts = value.timestamp * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
        payState.update(true)
      }
    }
  }

  // 定时器触发有两种场景 1.订单创建但是没有支付，会注册一个定时器； 2.已支付但是没有创建订单。
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderLog, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 1.订单创建但是没有支付，会注册一个定时器；
    if (createState.value()) {
      ctx.output(outputTag, OrderResult(ctx.getCurrentKey, "order timeout(not found pay log)"))
    }
    // 2.有支付数据但是没有创建数据，会注册一个定时器；
    else if (payState.value()) {
      ctx.output(outputTag, OrderResult(ctx.getCurrentKey, "order timeout(not found create log)"))
    }

    createState.clear()
    payState.clear()
    timerTsState.clear()
  }

}