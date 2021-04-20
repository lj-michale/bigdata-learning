package com.bidata.example.structuredstreaming

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Created by shirukai on 2019-03-23 11:29
 * 官方源码window()函数实现
 *
 *
 * The windows are calculated as below:
 * maxNumOverlapping <- ceil(windowDuration / slideDuration)
 * for (i <- 0 until maxNumOverlapping)
 * windowId <- ceil((timestamp - startTime) / slideDuration)
 * windowStart <- windowId * slideDuration + (i - maxNumOverlapping) * slideDuration + startTime
 * windowEnd <- windowStart + windowDuration
 * return windowStart, windowEnd
 */
object WindowSourceFunctionExample {
  val TARGET_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  /**
   * 计算窗口ID
   *
   * @param t 事件时间
   * @param s 滑动间隔
   * @return id
   */
  def calculateWindowId(t: Long, s: Long): Int = Math.ceil(t.toDouble / s.toDouble).toInt

  /**
   * 计算窗口开始时间
   *
   * @param windowId          窗口ID
   * @param s                 滑动间隔
   * @param maxNumOverlapping 最大重叠窗口个数
   * @param numOverlapping    当前重叠数
   * @return start
   */
  def calculateWindowStart(windowId: Int, s: Long, maxNumOverlapping: Int, numOverlapping: Int): Long =
    windowId * s + (numOverlapping - maxNumOverlapping) * s

  /**
   * 计算窗口结束时间
   *
   * @param start 开始时间
   * @param w     窗口大小
   * @return end
   */
  def calculateWindowEnd(start: Long, w: Long): Long = start + w

  /**
   * 计算最多的窗口重叠个数
   * 思路：当前事件时间到所在窗口的结束时间 / 滑动间隔 向上取整，即是窗口个数
   *
   * @param w 窗口间隔
   * @param s 滑动间隔
   * @return 窗口个数
   */
  def calculateMaxNumOverlapping(w: Long, s: Long): Int = Math.ceil(w.toDouble / s.toDouble).toInt

  /**
   * 模拟计算某个时间的窗口
   *
   * @param eventTime      事件时间 毫秒级时间戳
   * @param windowDuration 窗口间隔 字符串表达式："10 seconds" or "10 minutes"
   * @param slideDuration  滑动间隔 字符串表达式："10 seconds" or "10 minutes"
   * @return List
   */
  def window(eventTime: Long, windowDuration: String, slideDuration: String): List[String] = {

    // Format window`s interval by CalendarInterval, e.g. "10 seconds" => "10000"
    val windowInterval = CalendarInterval.fromString(s"interval $windowDuration").milliseconds()

    // Format slide`s interval by CalendarInterval, e.g. "10 seconds" => "10000"
    val slideInterval = CalendarInterval.fromString(s"interval $slideDuration").milliseconds()

    if (slideInterval > windowInterval) throw
      new RuntimeException(s"The slide duration ($slideInterval) must be less than or equal to the windowDuration ($windowInterval).")

    val maxNumOverlapping = calculateMaxNumOverlapping(windowInterval, slideInterval)

    val windowId = calculateWindowId(eventTime, slideInterval)

    List.tabulate(maxNumOverlapping)(x => {
      val start = calculateWindowStart(windowId, slideInterval, maxNumOverlapping, x)
      val end = calculateWindowEnd(start, windowInterval)
      (start, end)
    }).filter(x => x._1 < eventTime && x._2 > eventTime)
      .map(x =>
        s"[${TARGET_FORMAT.format(new Date(x._1))}, ${TARGET_FORMAT.format(new Date(x._2))}]"
      )
  }

  def main(args: Array[String]): Unit = {
    window(1553320704000L, "10 minutes", "2 minutes").foreach(println)
  }

}
