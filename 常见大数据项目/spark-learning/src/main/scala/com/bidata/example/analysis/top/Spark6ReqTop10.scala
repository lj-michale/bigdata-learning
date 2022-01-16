package com.bidata.example.analysis.top

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 热门品类top10
 */
object Spark6ReqTop10 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // todo... 存在大量的reduceByKey操作
    // reduceByKey 聚合算子，spark会提供缓存优化

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    actionRDD.cache()

    // 2. top10 品类
    val top10Ids: Array[String] = top10Category(actionRDD)

    // 3. 过滤原始数据，保留点击和前十品类的id
    val filterRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1" && top10Ids.contains(datas(6))
      }
    )

    // 4. 根据品类id和sessionId进行点击量的统计
    val reduceRDD: RDD[((String, String), Int)] = filterRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 5. 转换结构
    // ((品类ID，Sid), sum) => (品类ID, (Sid， sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((cid, sid), sum) => (cid, (sid, sum))
    }

    // 分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()
    val result: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )

    // 6. 打印
    result.collect().foreach(println)
    sc.stop()
  }

  def top10Category(actionRDD: RDD[String]) = {
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
    resultRDD.map(_._1)
  }

}
