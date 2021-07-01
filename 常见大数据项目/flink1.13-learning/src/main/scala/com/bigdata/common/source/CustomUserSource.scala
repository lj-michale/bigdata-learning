package com.bigdata.common.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @author lj.michale
 * @description 自定义用户相关信息
 * @date 2021-07-01
 */
case class CustomUser()

class CustomUserSource extends SourceFunction[CustomUser]{

  override def run(sc: SourceFunction.SourceContext[CustomUser]): Unit = {

  }

  override def cancel(): Unit = {

  }



}
