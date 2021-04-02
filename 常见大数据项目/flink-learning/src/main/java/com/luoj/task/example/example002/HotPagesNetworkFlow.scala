package com.luoj.task.example.example002

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Description: 热门页面浏览量统计
 * @Author:
 * @Data:
 */
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, cnt: Long)

object HotPagesNetworkFlow {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\apache.log")
    //val inputStream = env.socketTextStream("192.168.32.242", 7777)
    val dataStream = inputStream.map(data => {
      val arr = data.split(" ")
      val ts = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(arr(3)).getTime
      ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
    })

    val aggStream = dataStream.filter(_.method == "GET").filter(data => {
        val pattern = "^((?!\\.(css|js)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(10))

    dataStream.print("dataStream")
    aggStream.print("aggStream")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")
    resultStream.print("resultStream")

    env.execute("hot page")

  }

}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  def createAccumulator: Long = 0
  def add(in: ApacheLogEvent, acc: Long): Long = acc + 1
  def getResult(acc: Long): Long = acc
  def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotPages(topSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  //  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

  override def processElement(input: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    //    pageViewCountListState.add(input)
    pageViewCountMapState.put(input.url, input.cnt)
    // 定义两个定时器，一个用于计算结果，一个用于清空状态
    ctx.timerService().registerEventTimeTimer(input.windowEnd + 1)
    ctx.timerService().registerEventTimeTimer(input.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    val allPageListCounts: ListBuffer[PageViewCount] = ListBuffer()
    //    val iterator = pageViewCountListState.get().iterator()
    //    while (iterator.hasNext) {
    //      allPageListCounts += iterator.next()
    //    }

    val allPageListCounts: ListBuffer[(String,Long)] = ListBuffer()
    val iter = pageViewCountMapState.entries().iterator()
    while(iter.hasNext){
      val element = iter.next()
      allPageListCounts += ((element.getKey,element.getValue))
    }

    //如果窗口关闭，则清空状态
    if (ctx.getCurrentKey + 60000L == timestamp) {
      pageViewCountMapState.clear()
      return
    }

    // 排序
    val sortedPageListCounts = allPageListCounts.sortWith(_._2 > _._2).take(topSize)
    // 格式化为String类型
    val result: StringBuffer = new StringBuffer
    result.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedPageListCounts.indices) {
      val currentPageCounts = sortedPageListCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("页面url = ").append(currentPageCounts._1).append("\t")
        .append("热门度 = ").append(currentPageCounts._2).append("\n")
    }

    result.append("====================================\n\n")

    Thread.sleep(1000)

    out.collect(result.toString)
  }
}