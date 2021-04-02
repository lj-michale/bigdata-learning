package com.luoj.task.example.example002

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Description: 筛选出乱序数据中两秒内连续两次登录失败数据(设置定时器，如果两秒内有2条以上登录失败则报警)
 *              存在逻辑错误，正常需要处理乱序数据，而且到达最大次数直接输出结果而非等到2秒后输出。
 * @Author:
 * @Data:
 */
case class LoginLog(userId:Long,ip:String,eventType: String,timestamp:Long)
case class LoginFailWarning(userId:Long,firstFailTime:Long,lastFailTime:Long,warningMsg:String)

object LoginFail {

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

    val resultStream = dataStream.keyBy(_.userId)
      .process(new LoginFailWarningResult(2))

    resultStream.print("warning")
    env.execute("login fail")

  }
}

class LoginFailWarningResult(maxFailTime:Long) extends KeyedProcessFunction[Long,LoginLog,LoginFailWarning]{
  lazy val loginFailList: ListState[LoginLog] = getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("loginfail-list", classOf[LoginLog]))
  lazy val timerTs:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts",classOf[Long]))

  // 遇到一个登录失败的数据就设置一个两秒的定时器，并将数据放入状态中。如果遇到登录成功，就
  override def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    if(value.eventType.equals("fail")) {
      loginFailList.add(value)
      if(timerTs.value() == 0L){
        val ts:Long = value.timestamp*1000 + 2000
        ctx.timerService().registerEventTimeTimer(ts)
        timerTs.update(ts)
      }
    }else{
      ctx.timerService().deleteEventTimeTimer(timerTs.value())
      loginFailList.clear()
      timerTs.clear()
    }
  }

  // 如果登录失败个数超过限制，则报警；最后清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginLog, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val iter = loginFailList.get().iterator()
    val allLoginFailList = new ListBuffer[LoginLog]
    while(iter.hasNext) {
      allLoginFailList += iter.next()
    }

    if(allLoginFailList.length >= maxFailTime){
      out.collect(LoginFailWarning(ctx.getCurrentKey,allLoginFailList.head.timestamp,allLoginFailList.last.timestamp,"连续登录失败"+maxFailTime+"次，进行报警"))
    }

    allLoginFailList.clear()
    timerTs.clear()
  }
}