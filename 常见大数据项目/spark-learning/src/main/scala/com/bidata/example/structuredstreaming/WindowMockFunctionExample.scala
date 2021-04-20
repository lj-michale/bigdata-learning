package com.bidata.example.structuredstreaming


import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Created by
 * 模拟window()函数实现
 */
object WindowMockFunctionExample {
  val TARGET_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
   * 计算最近窗口开始时间
   * 思路：
   * 默认以0为第一个窗口的开始时间，滑动时间为s
   * 第二个窗口的开始时间:0+s
   * 第三个窗口的开始时间:0+2s
   * 第四个窗口的开始时间:0+3s
   * ……
   * 第n个窗口的开始时间:(n-1)s
   *
   * 设时间t落在第n个窗口，根据上面的公式，t所在的窗口开始时间为：startTime_n = (n-1)s
   * 再设时间t距离所在窗口开始时间为x，那么窗口开始时间也可以表示为：startTime_n = t-x
   * t-x = (n-1)s
   * n-1 = (t-x)/s
   * n-1为整数，由上面式子可以得出，x = t % s
   * 所以：startTime_n = t - (t % s)
   *
   * @param t 事件时间
   * @param s 滑动间隔
   * @return 窗口开始时间
   */
  def calculateWindowStartTime(t: Long, s: Long): Long = t - (t % s)

  /**
   * 计算窗口个数
   * 思路：当前事件时间到所在窗口的结束时间 / 滑动间隔 向上取整，即是窗口个数
   * @param w 窗口间隔
   * @param s 滑动间隔
   * @return 窗口个数
   */
  def calculateWindowNumber(t: Long, w: Long, s: Long): Int = ((w - (t % s) + (s - 1)) / s).toInt

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

    val windowStartTime = calculateWindowStartTime(eventTime, slideInterval)
    val windowNumber = calculateWindowNumber(eventTime,windowInterval, slideInterval)

    List.tabulate(windowNumber)(x => {
      val start = windowStartTime - x * slideInterval
      val end = start + windowInterval
      s"[${TARGET_FORMAT.format(new Date(start))}, ${TARGET_FORMAT.format(new Date(end))}]"
    }).reverse
  }

  def main(args: Array[String]): Unit = {
    window(1553320704000L, "10 minutes", "3 minutes").foreach(println)
  }

}
