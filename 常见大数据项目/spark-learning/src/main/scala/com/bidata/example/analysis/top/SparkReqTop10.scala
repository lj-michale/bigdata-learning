package com.bidata.example.analysis.top

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 热门品类top10
 */
object SparkReqTop10 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    // 2. 统计品类的点击数量：（品类id，点击数量）
    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 统计品类的下单数量：（品类id，下单数量）
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    // orderid => 1,2,3
    // 【（1，1），（2，1），（3，1）】
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cidstr: String = datas(8)
        val cids: Array[String] = cidstr.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_+_)

    // 4. 统计品类的支付数量：（品类id，支付数量）
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    // orderid => 1,2,3
    // 【（1，1），（2，1），（3，1）】
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cidstr: String = datas(10)
        val cids: Array[String] = cidstr.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_+_)

    // 5. 将品类进行排序，并且取前十名
    //    点击数量排序，下单数量排序，支付数量排序
    //    元组排序：先比较第一个，再比较第二个，再比较第三个
    //    （品类ID，（点击数量，下单数量，支付数量））
    // join, zip, leftOuterJoin, cogroup
    // 看了一圈，这里只有cogroup合适 = connect + group
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
    clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        val clickCount = if (clickIter.iterator.hasNext) clickIter.iterator.next() else 0
        val orderCount = if (orderIter.iterator.hasNext) orderIter.iterator.next() else 0
        val payCount = if (payIter.iterator.hasNext) payIter.iterator.next() else 0
        (clickCount, orderCount, payCount)
      }
    }
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()

  }

}
