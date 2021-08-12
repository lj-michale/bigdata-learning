package com.bigdata.task.learn.window.trigger

import java.text.SimpleDateFormat

import com.bigdata.task.learn.bean.eventInputDT
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


class CustomTrigger extends Trigger[eventInputDT, TimeWindow] {
  //触发计算的最大数量
  private var maxCount: Long = _
  //定时触发间隔时长 (ms)
  private var interval: Long = 60 * 1000
  //记录当前数量的状态
  private lazy val countStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("counter", new Sum, classOf[Long])
  //记录执行时间定时触发时间的状态
  private lazy val processTimerStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("processTimer", new Update, classOf[Long])
  //记录事件时间定时器的状态
  private lazy val eventTimerStateDescriptor: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("eventTimer", new Update, classOf[Long])

  def this(maxCount: Int) {
    this()
    this.maxCount = maxCount
  }

  def this(maxCount: Int, interval: Long) {
    this(maxCount)
    this.interval = interval
  }

  override def onElement(element: eventInputDT, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val countState = ctx.getPartitionedState(countStateDescriptor)
    //计数状态加1
    countState.add(1L)

    //如果没有设置事件时间定时器，需要设置一个窗口最大时间触发器，这个目的是为了在窗口清除的时候 利用时间时间触发计算，否则可能会缺少部分数据
    if (ctx.getPartitionedState(eventTimerStateDescriptor).get() == 0L) {
      ctx.getPartitionedState(eventTimerStateDescriptor).add(window.maxTimestamp())
      ctx.registerEventTimeTimer(window.maxTimestamp())
    }

    if (countState.get() >= this.maxCount) {
      //达到指定指定数量
      //删除事件时间定时触发的状态
      ctx.deleteProcessingTimeTimer(ctx.getPartitionedState(processTimerStateDescriptor).get())
      //清空计数状态
      countState.clear()
      //触发计算
      TriggerResult.FIRE
    } else if (ctx.getPartitionedState(processTimerStateDescriptor).get() == 0L) {
      //未达到指定数量，且没有指定定时器，需要指定定时器
      //当前定时器状态值加上间隔值
      ctx.getPartitionedState(processTimerStateDescriptor).add(ctx.getCurrentProcessingTime + interval)
      //注册定执行时间定时器
      ctx.registerProcessingTimeTimer(ctx.getPartitionedState(processTimerStateDescriptor).get())
      TriggerResult.CONTINUE
    } else {
      TriggerResult.CONTINUE
    }
  }

  // 执行时间定时器触发
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (ctx.getPartitionedState(countStateDescriptor).get() > 0 && (ctx.getPartitionedState(processTimerStateDescriptor).get() == time)) {
      println(s"数据量未达到 $maxCount ,由执行时间触发器 ctx.getPartitionedState(processTimerStateDescriptor).get()) 触发计算")
      ctx.getPartitionedState(processTimerStateDescriptor).clear()
      ctx.getPartitionedState(countStateDescriptor).clear()
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
  }

  //事件时间定时器触发
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if ((time >= window.maxTimestamp()) && (ctx.getPartitionedState(countStateDescriptor).get() > 0L)) { //还有未触发计算的数据
      println(s"事件时间到达最大的窗口时间，并且窗口中还有未计算的数据:${ctx.getPartitionedState(countStateDescriptor).get()}，触发计算并清除窗口")
      ctx.getPartitionedState(eventTimerStateDescriptor).clear()
      TriggerResult.FIRE_AND_PURGE
    } else if ((time >= window.maxTimestamp()) && (ctx.getPartitionedState(countStateDescriptor).get() == 0L)) { //没有未触发计算的数据
      println("事件时间到达最大的窗口时间，但是窗口中没有有未计算的数据，清除窗口 但是不触发计算")
      TriggerResult.PURGE
    } else {
      TriggerResult.CONTINUE

    }
  }

  //窗口结束时清空状态
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // println(s"清除窗口状态，定时器")
    ctx.deleteEventTimeTimer(ctx.getPartitionedState(eventTimerStateDescriptor).get())
    ctx.deleteProcessingTimeTimer(ctx.getPartitionedState(processTimerStateDescriptor).get())
    ctx.getPartitionedState(processTimerStateDescriptor).clear()
    ctx.getPartitionedState(eventTimerStateDescriptor).clear()
    ctx.getPartitionedState(countStateDescriptor).clear()
  }

  //更新状态为累加值
  class Sum extends ReduceFunction[Long] {
    override def reduce(value1: Long, value2: Long): Long = value1 + value2
  }

  //更新状态为取新的值
  class Update extends ReduceFunction[Long] {
    override def reduce(value1: Long, value2: Long): Long = value2
  }

}
