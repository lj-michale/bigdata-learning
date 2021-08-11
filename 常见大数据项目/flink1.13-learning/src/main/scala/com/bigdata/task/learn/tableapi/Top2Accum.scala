package com.bigdata.task.learn.tableapi

import java.lang.{Integer => JInteger}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.TableAggregateFunction

/**
 * Top2 Accumulator。
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
}
