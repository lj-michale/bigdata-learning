package com.bidata.example.analysis.top

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 热门品类top10
 */
object Spark3ReqTop10 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // todo... 存在大量的reduceByKey操作
    // reduceByKey 聚合算子，spark会提供缓存优化

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    // 2. 将数据转换结构
    //  点击场合：（品类ID，(1，0，0)）
    //  下单场合：（品类ID，(0，1，0)）
    //  支付场合：（品类ID，(0，0，1)）
    //  这样一次reduceByKey，一次聚合
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击场合
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单场合
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付场合
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    // 3. 将相同品类id的数据进行分组聚合
    // （品类ID，(点击数量，下单数量，支付数量)）
    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    )

    // 4. 将统计结果进行降序排列，取前十名
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    // 5. 将结果采集到控制台打印出来
    resultRDD.foreach(println)
    sc.stop()
  }

}
