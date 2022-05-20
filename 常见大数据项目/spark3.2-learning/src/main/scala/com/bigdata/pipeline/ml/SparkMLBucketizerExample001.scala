package com.bigdata.pipeline.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

/**
 * @descri Bucketizer transforms a column of continuous features to a column of feature buckets, where the buckets are specified by users. It takes a parameter:
 *
 * @author lj.michale
 * @date 2022-05-20
 */
object SparkMLBucketizerExample001 {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.adaptive.enable", "true")

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("SparkMLBucketizerExample001")
      .getOrCreate()

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
    bucketedData.show()

    val splitsArray = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.0, 0.3, Double.PositiveInfinity))

    val data2 = Array(
      (-999.9, -999.9),
      (-0.5, -0.2),
      (-0.3, -0.1),
      (0.0, 0.0),
      (0.2, 0.4),
      (999.9, 999.9))
    val dataFrame2 = spark.createDataFrame(data2).toDF("features1", "features2")

    val bucketizer2 = new Bucketizer()
      .setInputCols(Array("features1", "features2"))
      .setOutputCols(Array("bucketedFeatures1", "bucketedFeatures2"))
      .setSplitsArray(splitsArray)

    // Transform original data into its bucket index.
    val bucketedData2 = bucketizer2.transform(dataFrame2)

    println(s"Bucketizer output with [" +
      s"${bucketizer2.getSplitsArray(0).length-1}, " +
      s"${bucketizer2.getSplitsArray(1).length-1}] buckets for each input column")
    bucketedData2.show()

    spark.stop()

  }


}
