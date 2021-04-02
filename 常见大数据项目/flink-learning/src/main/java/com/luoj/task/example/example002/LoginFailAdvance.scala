package com.luoj.task.example.example002

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Description:
 * @Author:
 * @Data:
 */
object LoginFailAdvance {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = getClass.getResource("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\LoginLog.csv")
    val inputStream = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\LoginLog.csv")

    val dataStream = inputStream.map(data => {
        val arr = data.split(",")
        LoginLog(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(3)) {
        override def extractTimestamp(element: LoginLog): Long = element.timestamp * 1000
      })

    val resultStream = dataStream
      .keyBy(_.userId)
      .process(new LoginFailAdvanceResult(2))

    resultStream.print("warning")
    env.execute("login fail advance")

  }
}

// 如果到达的是失败数据，则判断时间间隔是否小于2s，是则存入状态并判断是否达到最大次数，达到则输出并清空状态；如果是成功数据，则清空状态。
class LoginFailAdvanceResult(maxFailTime: Int) extends KeyedProcessFunction[Long, LoginLog, LoginFailWarning] {
  lazy val loginFailList: ListState[LoginLog] = getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("loginfail-list", classOf[LoginLog]))

  override def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    if (value.eventType.equals("fail")) {
      val iter = loginFailList.get().iterator()
      val allLoginFailLog = new ListBuffer[LoginLog]
      while (iter.hasNext) {
        allLoginFailLog += iter.next()
      }
      // 如果新数据比第一个数据的时间差小于2s，则插入数据
      if(allLoginFailLog.nonEmpty) {
        if (value.timestamp - allLoginFailLog.head.timestamp <= 2) {
          loginFailList.add(value)
          // 判断累计的信息数量，大于maxFailTime则输出报警
          if (allLoginFailLog.size >= maxFailTime - 1) {
            out.collect(LoginFailWarning(ctx.getCurrentKey, allLoginFailLog.head.timestamp, value.timestamp, "连续登录失败" + maxFailTime + "次，进行报警"))
            loginFailList.clear()
          }
        } else {
          // 如果超时则清空状态
          loginFailList.clear()
        }
      }else{
        loginFailList.add(value)
      }
    } else {
      // 如果是成功信息，则清空状态
      loginFailList.clear()
    }
    ctx.timestamp()
  }

}