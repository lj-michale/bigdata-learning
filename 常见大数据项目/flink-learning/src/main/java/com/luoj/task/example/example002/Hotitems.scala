package com.luoj.task.example.example002

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Description: 需求：每五秒钟输出最近一个小时的topN点击商品  (步骤：1.转换为样例类，设置水位线 2.过滤点击事件，开1小时5分钟的滑动窗口对同类商品进行聚合 3.对同窗口的商品进行排序)
 * @Author:
 * @Data:
 */
// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, catagoryId: Int, behavior: String, timestamp: Long)
// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object Hotitems {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取数据并转换成样例类,并指定当前时间戳为事件时间
    //    val inputStream: DataStream[String] = env.readTextFile("C:\\Users\\Administrator\\Desktop\\不常用的项目\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    // 从Kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.17.11.31:9092,172.17.11.30:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")


    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("jiguang_001", new SimpleStringSchema(), properties))

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val split = data.split(",")
        UserBehavior(split(0).toLong, split(1).toLong, split(2).toInt, split(3), split(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 得到窗口的聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") // 过滤pv行为
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new ItemViewWindowResult())

    //
    val resultStream = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItems(10))

    resultStream.print()

    env.execute("hot items")
  }
}

// 自定义预聚合函数Aggregate
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator: Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

// 自定义窗口全局函数
class ItemViewWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0 // 因为key只有一个，所以Tuple是Tuple1类型的，转型后获取值
    val windowEnd = window.getEnd // 获取窗口的结束时间
    val count = input.iterator.next() // 全局窗口函数会记录所有的输入值，此处是传入的acc值，只有一个

    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

// 自定义的KeyedProcessFunction,进行状态编程
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  // 先定义状态： ListState
  private var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化状态
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCountList", classOf[ItemViewCount]))
  }

  // 数据处理并设置定时器
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据，直接加入ListState
    itemViewCountListState.add(value)
    //注册一个windowEnd + 1 之后出发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 出发定时器时处理
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()

    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }

    itemViewCountListState.clear()

    // 获取前topSize名的商品
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize) // 科里化

    // 格式化为String类型
    val result: StringBuffer = new StringBuffer
    result.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }

    result.append("====================================\n\n")

    Thread.sleep(1000)

    out.collect(result.toString)
  }

}