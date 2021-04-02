package com.luoj.task.example.example002

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Description:
 * @Author:
 * @Data:
 */
object LoginFailCEP {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = getClass.getResource("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\LoginLog.csv")
    val inputStream = env.readTextFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\datasets\\LoginLog.csv")

    val dataStream = inputStream.map(data => {
        val arr = data.split(",")
        LoginLog(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(2)) {
        override def extractTimestamp(element: LoginLog): Long = element.timestamp * 1000
      })

    val pattern = Pattern
      .begin[LoginLog]("firstFail").where(_.eventType.equals("fail"))
      .next("secondFail").where(_.eventType.equals("fail"))
      .within(Time.seconds(2))

    val resultStream = CEP.pattern(dataStream.keyBy(_.userId), pattern)
      .select(new PatternSelectResult)

    resultStream.print("warning")
    env.execute("login fail cep")
  }
}

class PatternSelectResult extends PatternSelectFunction[LoginLog,LoginFailWarning] {
  override def select(map: util.Map[String, util.List[LoginLog]]): LoginFailWarning = {
    val firstFailLog = map.get("firstFail").get(0)
    val secondFailLog = map.get("secondFail").get(0)
    LoginFailWarning(firstFailLog.userId,firstFailLog.timestamp,secondFailLog.timestamp,"login fail warning")
  }
}