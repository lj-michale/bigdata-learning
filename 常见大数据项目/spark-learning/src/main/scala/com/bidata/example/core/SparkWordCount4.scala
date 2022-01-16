package com.bidata.example.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkWordCount4 {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("WordCount").getOrCreate()
    val sc: SparkContext = session.sparkContext
    wordCount1(sc)

    sc.stop()
  }

  // groupBy
  def wordCount1(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Word", "Hello China"))
    val word: RDD[String] = rdd.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = word.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = wordGroup.mapValues(iter => iter.size)
  }

  // groupByKey (效率不高， 有shuffle操作，如果数据量大，效率很受影响)
  def wordCount2(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Word", "Hello China"))
    val word: RDD[String] = rdd.flatMap(_.split(" "))
    val smallWC: RDD[(String, Int)] = word.map((_, 1))
    val wordGroup: RDD[(String, Iterable[Int])] = smallWC.groupByKey()
    val wordCount: RDD[(String, Int)] = wordGroup.mapValues(iter => iter.size)
  }

  // reduceByKey
  def wordCount3(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Word", "Hello China"))
    val word: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = word.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey((_: Int) + (_: Int))
  }

  // aggregateByKey (分区内和分区间计算规则相同， 可简化成foldByKey)
  def wordCount4(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Word", "Hello China"))
    val word: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = word.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
  }

  // foldByKey
  def wordCount5(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Word", "Hello China"))
    val word: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = word.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
  }


  // countByValue
  def wordCount6(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Word", "Hello China"))
    val word: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = word.countByValue()
  }

  // folByKey
  def wordCount7(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Word", "Hello China"))
    val word: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = word.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
  }

}
