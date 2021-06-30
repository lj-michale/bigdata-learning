package com.luoj.task.learn.partition

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * 创建自定义并行度为1的source
 * 实现从1开始产生递增数字
 */
class MyNoParallelSourceScala extends SourceFunction[Long]{

  var count = 1L
  var isRunning = true

  override def run(ctx: SourceContext[Long]) = {
    while(isRunning){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }

  }

  override def cancel() = {
    isRunning = false
  }
}