package com.luoj.task.learn.udf

import org.apache.flink.table.functions.AggregateFunction
import scala.collection.mutable.ListBuffer

class MedianUdaf extends AggregateFunction[Double, ListBuffer[Double]] {

  /*
    * 具有初始值的累加器
    * 初始化AggregateFunction的accumulator。
    * 系统在第一个做aggregate计算之前调用一次这个方法。
    */
  override def createAccumulator(): ListBuffer[Double] = new ListBuffer[Double]()

  /*
  * 系统在每次aggregate计算完成后调用这个方法。
   */
  override def getValue(accumulator: ListBuffer[Double]) = {

    val length = accumulator.size
    val med = (length / 2)
    val seq = accumulator.sorted

    try {
      length % 2 match {
        case 0 => (seq(med) + seq(med - 1)) / 2
        case 1 => seq(med)
      }
    } catch {
      case e: Exception => seq.head
    }
  }


  /*
  * UDAF必须包含1个accumulate方法。
  * 您需要实现一个accumulate方法，来描述如何计算用户的输入的数据，并更新到accumulator中。
  * accumulate方法的第一个参数必须是使用AggregateFunction的ACC类型的accumulator。
  * 在系统运行过程中，底层runtime代码会把历史状态accumulator，
  * 和指定的上游数据（支持任意数量，任意类型的数据）做为参数，一起发送给accumulate计算。
   */
  def accumulate(accumulator: ListBuffer[Double], i: Double) = {
    accumulator.append(i)
  }

  /*
  * 使用merge方法把多个accumulator合为1个accumulator
  * merge方法的第1个参数，必须是使用AggregateFunction的ACC类型的accumulator，而且第1个accumulator是merge方法完成之后，状态所存放的地方。
  * merge方法的第2个参数是1个ACC type的accumulator遍历迭代器，里面有可能存在1个或者多个accumulator。
   */
  def merge(accumulator: ListBuffer[Double], its: Iterable[ListBuffer[Double]]) = {
    its.foreach(i => accumulator ++ i)
  }

  // 返回结果的类型（一般情况下可以不用自己实现，但是在涉及到更复杂的类型是可能会用到）
  //  override def getResultType = createTypeInformation[Double]

  // 返回中间结果的类型
  //  override def getAccumulatorType = createTypeInformation[ListBuffer[Double]]

}

