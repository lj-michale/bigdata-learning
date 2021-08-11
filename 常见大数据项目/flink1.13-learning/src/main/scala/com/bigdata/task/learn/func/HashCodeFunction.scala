package com.bigdata.task.learn.func

import org.apache.flink.table.api._
import org.apache.flink.table.functions.FunctionContext
import org.apache.flink.table.functions.ScalarFunction

class HashCodeFunction extends ScalarFunction {

  private var factor: Int = 0

  override def open(context: FunctionContext): Unit = {
    // 获取参数 "hashcode_factor"
    // 如果不存在，则使用默认值 "12"
    factor = context.getJobParameter("hashcode_factor", "12").toInt
  }

  def eval(s: String): Int = {
    s.hashCode * factor
  }
}

