package com.luoj.task.example.example002

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Description: 需求：实现一小时内网站总浏览量PV的统计
 * @Author:
 * @Data:
 */
case class UserBehavior(userId: Long, itemId: Long, catagoryId: Int, behavior: String, timestamp: Long)

case class PvCount(windowEnd: Long, count: Long)

object PageView {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\UserBehavior.csv")
    val inputStream = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\UserBehavior.csv")

    val dataStream = inputStream.map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    val aggStream = dataStream
      .filter(_.behavior == "pv")
      .map(new MyMapper)
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg, new PvCountWindowResult)

    //    aggStream.print("aggStream")

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new PvCountProcessResult)

    resultStream.print("result")

    env.execute("pv count")
  }
}

class MyMapper extends MapFunction[UserBehavior, (String, Long)] {
  override def map(userBehavior: UserBehavior): (String, Long) = {
    (Random.nextString(10), 1)
  }
}

class PvCountAgg extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class PvCountWindowResult extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}

class PvCountProcessResult extends KeyedProcessFunction[Long, PvCount, PvCount] {

  lazy val totalPvCountResultState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("ValueState", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
    val count = totalPvCountResultState.value()
    totalPvCountResultState.update(count + value.count)

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    val totalPvCount = totalPvCountResultState.value()
    out.collect(PvCount(ctx.getCurrentKey.toLong, totalPvCount))
    totalPvCountResultState.clear()
  }
}