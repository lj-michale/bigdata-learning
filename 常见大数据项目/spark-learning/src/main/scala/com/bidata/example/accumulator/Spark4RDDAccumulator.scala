package com.bidata.example.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
 * 自定义累加器
 */
object Spark4RDDAccumulator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("hello", "scala", "spark", "china", "china"))

    /**
     * 累加器 wordCount
     * 创建累加器对象
     */
    val wcAcc = new MyAccumulator
    // 向 spark 注册自定义累加器
    sc.register(wcAcc, "wordCountAcc")

    rdd.foreach(
      word => {
        // 使用累加器对单词进行计数
        wcAcc.add(word)
      }
    )

    // 获取累加器结果
    println(wcAcc.value)

    sc.stop()
  }

  /**
   * 1. 继承 AccumulatorV2，定义泛型
   * IN：累加器输入的数据类型String
   * OUT：累加器返回的数据类型 mutable.Map[String, Long]
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    // 判断是否为初始状态
    override def isZero: Boolean = wcMap.isEmpty

    // 复制一个新的累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()

    // 重置
    override def reset(): Unit = wcMap.clear()


    // 累加
    override def add(word: String): Unit = {
      val newCount = wcMap.getOrElse(word, 0l) + 1
      wcMap.update(word, newCount)
    }

    // 合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach{
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0l) + count
          map1.update(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = wcMap
  }

}
