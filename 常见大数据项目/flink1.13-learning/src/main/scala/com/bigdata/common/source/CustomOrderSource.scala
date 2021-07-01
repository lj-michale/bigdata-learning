package com.bigdata.common.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @author lj.michale
 * @description 自定义订单相关信息
 * @date 2021-07-01
 */

case class CustomOrder()

class CustomOrderSource extends SourceFunction[CustomOrder]{

  //定义一个flag 用来标记数据源是否正常发出数据
  var running = true
  var currentTime:Long = System.currentTimeMillis()

  override def run(sc: SourceFunction.SourceContext[CustomOrder]): Unit = {



  }

  override def cancel(): Unit = {

  }

}
