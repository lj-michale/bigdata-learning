package com.bidata.example.analysis.top

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 热门品类top10
 * 使用累加器，消除shuffle操作
 */
object Spark4ReqTop10 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    // 2. 创建累加器并注册
    val acc = new HotCategoryAccumulator
    sc.register(acc, "hotCategory")

    // 3.数据结构转换
    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击场合
          acc.add(datas(6), "click")
        } else if (datas(8) != "null") {
          // 下单场合
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(id => acc.add(id, "order"))
        } else if (datas(10) != "null") {
          // 支付场合
          val ids: Array[String] = datas(10).split(",")
          ids.foreach(id => acc.add(id, "pay"))
        }
      }
    )

    // 4.取累加器的值
    val accVal: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)
    val sort: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )

    // 5. 将结果打印
    sort.take(10).foreach(println)
    sc.stop()
  }

  case class HotCategory(cid:String, var clickCnt:Int, var orderCnt:Int, var payCnt:Int)

  /**
   * 自定义累加器
   * 1. 继承 AccumulatorV2，定义范型
   *    IN：（品类ID，行为类型）
   *    OUT：mutable.Map[String, HotCategory]
   * 2. 重写方法
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val hotMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = hotMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = hotMap.clear()

    override def add(v: (String, String)): Unit = {
      val cid:String = v._1
      val actionType:String = v._2
      val category: HotCategory = hotMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      hotMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hotMap
      val map2 = other.value
      map2.foreach{
        case (cid, hc) => {
          val category:HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hotMap
  }

}
