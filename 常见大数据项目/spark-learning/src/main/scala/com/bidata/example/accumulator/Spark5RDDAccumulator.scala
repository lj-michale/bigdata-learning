package com.bidata.example.accumulator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object Spark5RDDAccumulator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    //
    //    // join会导致数据量几何倍数增长，并且会影响shuffle的性能，不推荐使用
    //    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    //    joinRDD.collect().foreach(println)

    // 采用别的方式代替join
    val map2 = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    // 广播变量：每个executor只会有一份map2，
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map2)

    rdd1.map{
      case (word, count) =>
        // 访问广播变量
        val l:Int = bc.value.getOrElse(word, 0)
        (word, (count, l))
    }.collect().foreach(println)

    sc.stop()

  }

}
