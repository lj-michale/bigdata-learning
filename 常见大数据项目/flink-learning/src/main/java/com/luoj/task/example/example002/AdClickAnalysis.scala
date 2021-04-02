package com.luoj.task.example.example002

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Description: 对一天内点击100次以上相同广告的用户输出为黑名单
 * @Author: lj.michale
 * @Data: 2021/2/3 15:51
 */
object AdClickAnalysis {

  def main(args: Array[String]): Unit = {

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = getClass.getResource("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\AdClickLog.csv")
//    val inputStream = env.readTextFile(source.getPath)
    val inputStream:DataStream[String] = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\AdClickLog.csv")

    val dataStream = inputStream.map(data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    // 判断用户是否多次点击同一广告，如果是则拦截日志并输出黑名单
    val processStream = dataStream.keyBy(data => (data.userId, data.adId))
      .process(new FliterBlackListUserResult(100))

    // 统计每个广告的用户点击数
    val resultStream = processStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg, new AdCountAggWindowResult)

    processStream.getSideOutput(new OutputTag[FilterBlackListUser]("black-list-user")).print("warn")
    resultStream.print("result")

    env.execute("ad black list")

  }

  class FliterBlackListUserResult(maxClick: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

    // 需要记录的状态有1.统计次数 2.该用户是否已经输出 3.
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
    lazy val isContained: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isContained", classOf[Boolean]))

    /**
     * processElement
     * */
    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {

      val curCount = countState.value()

      // 如果计数个数为0，说明第一次点击该广告，定义到当天24:00的定时器
      if (curCount == 0) {
        val ts = (System.currentTimeMillis() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        ctx.timerService().registerEventTimeTimer(ts)
      }

      // 如果计数大于maxClick个，判断是否已经输出该用户，如果没有，则输出到侧输出流
      if (curCount >= maxClick) {
        if (!isContained.value()) {
          isContained.update(true)
          ctx.output(new OutputTag[FilterBlackListUser]("black-list-user"), FilterBlackListUser(ctx.getCurrentKey._1, ctx.getCurrentKey._2, "该用户重复点击" + maxClick))
        }
        // 如果点击超过限制，该条日志不传到下游
        return
      }

      countState.update(curCount + 1)
      out.collect(value)
    }

    /**
     * 触发器中，清空保存的状态
     * */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
      isContained.update(false)
      countState.update(0)
    }

  }

  class AdCountAgg extends AggregateFunction[AdClickLog, Long, Long] {
    override def createAccumulator(): Long = 0L
    override def add(in: AdClickLog, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class AdCountAggWindowResult extends ProcessWindowFunction[Long, AdClickCountByProvince, String,TimeWindow]{
    /**
     * process
     * */
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
      out.collect(AdClickCountByProvince(context.window.getEnd.toString,key,elements.head))
    }
  }

  case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
  case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)
  case class FilterBlackListUser(userId: Long, adId: Long, msg: String)

}


