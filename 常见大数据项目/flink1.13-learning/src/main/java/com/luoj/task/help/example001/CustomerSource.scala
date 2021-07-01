package com.luoj.task.help.example001

import org.apache.flink.streaming.api.functions.source.SourceFunction


class CustomerSource extends SourceFunction[Tuple2[Long,Long]]{

  var count=1625048255867L
  var isRunning=true
  override def run(ctx: SourceFunction.SourceContext[Tuple2[Long,Long]]): Unit = {
    while(isRunning) {
      ctx.collect(new Tuple2(count,count))
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {

    isRunning=false
  }
}