package com.luoj.task.example.example002

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @Description: 统计UV，使用布隆过滤器保存用户信息
 * @Author:
 * @Data:
 */
case class UvCount(window: Long, count: Long)

object UniqueView {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = getClass.getResource("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\UserBehavior.csv")
    val inputStream = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\UserBehavior.csv")

    val dataStream = inputStream.map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    dataStream.filter("pv" == _.behavior)
      .map(user => ("pv", user.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new UvCountWithBloom)

    env.execute("uv count")
  }
}

class MyTrigger extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

class Bloom(size: Long) {
  private val cap = size

  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = value.charAt(i) + result * seed
    }
    // 结果需要在size范围内
    (cap - 1) & result
  }
}

class UvCountWithBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  lazy val jedis: Jedis = new Jedis("116.62.148.11", 6380)

  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    val now = LocalDateTime.now(ZoneId.of("Asia/Shanghai"))
    val date = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(now)

    val hkey = date + "_count"
    val bkey = date + "_bitmap"
    val windowEnd = context.window.getEnd.toString
    // 使用hash结构存储统计值
    val result = jedis.hget(hkey, windowEnd)
    var count = 0L
    if (result != null) {
      count = result.toLong
    }

    // 判断userId是否存在，如果不存在，则统计结果加一并存入bitmap中
    val lastEle: (String, Long) = elements.last
    val offset = bloom.hash(lastEle._2.toString, 61)
    val isExist: Boolean = jedis.getbit(bkey, offset)

    if (!isExist) {
      jedis.setbit(bkey, offset, true)
      jedis.hset(hkey, windowEnd, (count+1).toString)
    }
  }
}