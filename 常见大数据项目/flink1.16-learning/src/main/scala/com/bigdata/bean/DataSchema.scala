package com.bigdata.bean

object DataSchema {

  // 定义样例类，传感器id，时间戳，温度
  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  case class MarketingUserBehavior(id:String, behavior:String, channel:String, timestamp: Long)

  case class MarketingViewCount(startTs:String, endTs:String, channel:String, behavior:String, count:Int)
}
