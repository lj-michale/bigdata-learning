package com.bigdata.task.learn.func

import java.lang.{Integer => JInteger, Long => JLong}

import org.apache.flink.api.java.tuple.{Tuple, Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.AggregateFunction

/**
 * Accumulator for WeightedAvg.
 */
class WeightedAvgAccum {
  var sum: JLong = 0L
  var count: JInteger = 0
}
