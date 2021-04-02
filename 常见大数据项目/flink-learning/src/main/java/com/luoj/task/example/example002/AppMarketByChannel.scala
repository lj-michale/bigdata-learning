package com.luoj.task.example.example002

import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.util.Random

/**
 * @Description:
 * @Author:
 * @Data:
 */
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
case class MarketCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

class SimulatedSource() extends RichSourceFunction[MarketUserBehavior] {

  // 标志位
  var running = true
  // 定义用户行为和渠道的集合
  val behaviorSet: Seq[String] = Seq("view", "click", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    // 定义生成数据最大数量
    val maxCounts = Long.MaxValue
    var count = 0L
    // 产生数据
    while (running && count < maxCounts) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(50)
    }
  }

  override def cancel(): Unit = running = false

}

object AppMarketByChannel {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.addSource(new SimulatedSource)
    val dataStream = inputStream.assignAscendingTimestamps(_.timestamp)

    val resultStream = dataStream.filter(_.behavior != "uninstall")
      .keyBy(data => (data.behavior, data.channel))
      .timeWindow(Time.days(1), Time.seconds(5))
      .process(new AppMarketCount)

    resultStream.print("result")
    env.execute("app market")

  }

}

class AppMarketCount extends ProcessWindowFunction[MarketUserBehavior,MarketCount,(String,String),TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketCount]): Unit = {
    val start = context.window.getStart.toString
    val end = context.window.getEnd.toString
    val behavior = key._1
    val channel = key._2
    val count = elements.size
    out.collect(MarketCount(start,end,channel,behavior,count))
  }
}
