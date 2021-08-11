package com.bigdata.task.learn.func

import java.lang.{Integer => JInteger, Iterable => JIterable}

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

/**
 * Accumulator for top2.
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
}

/**
 * The top2 user-defined table aggregate function.
 */
class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Accum] {

  override def createAccumulator(): Top2Accum = {
    val acc = new Top2Accum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc
  }

  def accumulate(acc: Top2Accum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def merge(acc: Top2Accum, its: JIterable[Top2Accum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val top2 = iter.next()
      accumulate(acc, top2.first)
      accumulate(acc, top2.second)
    }
  }

  def emitValue(acc: Top2Accum, out: Collector[JTuple2[JInteger, JInteger]]): Unit = {
    // emit the value and rank
    if (acc.first != Int.MinValue) {
      out.collect(JTuple2.of(acc.first, 1))
    }
    if (acc.second != Int.MinValue) {
      out.collect(JTuple2.of(acc.second, 2))
    }
  }
}
