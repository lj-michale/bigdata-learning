package com.bigdata.task.learn.func

import java.lang.{Integer => JInteger, Long => JLong}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.AggregateFunction
/**
 * Weighted Average user-defined aggregate function.
 */
class WeightedAvg extends AggregateFunction[JLong, WeightedAvgAccum] {

  override def createAccumulator(): WeightedAvgAccum = {
    new WeightedAvgAccum
  }

  override def getValue(acc: WeightedAvgAccum): JLong = {
    if (acc.count == 0) {
      null
    } else {
      acc.sum / acc.count
    }
  }

  def accumulate(acc: WeightedAvgAccum, iValue: JLong, iWeight: JInteger): Unit = {
    acc.sum += iValue * iWeight
    acc.count += iWeight
  }

  def retract(acc: WeightedAvgAccum, iValue: JLong, iWeight: JInteger): Unit = {
    acc.sum -= iValue * iWeight
    acc.count -= iWeight
  }

  def merge(acc: WeightedAvgAccum, it: java.lang.Iterable[WeightedAvgAccum]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.count += a.count
      acc.sum += a.sum
    }
  }

  def resetAccumulator(acc: WeightedAvgAccum): Unit = {
    acc.count = 0
    acc.sum = 0L
  }

  override def getAccumulatorType: TypeInformation[WeightedAvgAccum] = {
    new TupleTypeInfo(classOf[WeightedAvgAccum], Types.LONG, Types.INT)
  }

  override def getResultType: TypeInformation[JLong] = Types.LONG
}

