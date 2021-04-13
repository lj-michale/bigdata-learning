package com.luoj.task.learn.udf

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class UdafSource extends RichSourceFunction[Double] {

  override def run(ctx: SourceFunction.SourceContext[Double]) = {
    while (true) {
      val d = scala.math.random
      ctx.collect(d)
      // 测试产生的每条数据以日志的格式打印出
//      val logger = Logger(this.getClass)
//      logger.error(s"当前值：$d")
      Thread.sleep(12000)
    }

  }

  override def cancel() = ???

}

