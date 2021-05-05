package com.bidata.example.accumulator

import org.apache.spark.util.AccumulatorV2

class FieldAccumulator extends AccumulatorV2[SumAandB, SumAandB]{

  private var A:Long = 0L
  private var B:Long = 0L

  // A、B同时为0则累加器值为0
  override def isZero: Boolean = A == 0L && B == 0L

  // 复制一个累加器
  override def copy(): AccumulatorV2[SumAandB, SumAandB] = {
    val newAcc = new FieldAccumulator
    newAcc.A = this.A
    newAcc.B = this.B
    newAcc
  }

  // 重置累加器为0
  override def reset(): Unit = { A = 0; B = 0L}

  // 用累加器记录汇总结果
  override def add(v: SumAandB): Unit = {
    A += v.A
    B += v.B
  }

  // 合并两个累加器
  override def merge(other: AccumulatorV2[SumAandB, SumAandB]): Unit = {
    other match {
      case o: FieldAccumulator =>{
        A += o.A
        B += o.B
      }
      case _ =>
    }
  }

  // Spark调用时返回结果
  override def value: SumAandB = SumAandB(A, B)
}
