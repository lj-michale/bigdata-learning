package com.bigdata.sql.udf

object AuroaUDF {

  /**
   * @descr getChannel
   * @param step
   * @param platform
   */
  def getChannel( step : String, platform: String): String ={
    val channel = step match {
      case _ => ""
    }
    channel
  }

}
