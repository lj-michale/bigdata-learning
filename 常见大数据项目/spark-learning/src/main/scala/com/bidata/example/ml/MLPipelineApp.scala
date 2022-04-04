package com.bidata.example.ml

import java.lang

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @descr MLPipelineApp
 * @author lj.michale
 * @date 2021-07
 */
object MLPipelineApp {

  case class Activity(label: Double,
                      accelXHand: Double,  accelYHand: Double,  accelZHand: Double,
                      accelXChest: Double, accelYChest: Double, accelZChest: Double,
                      accelXAnkle: Double, accelYAnkle: Double, accelZAnkle: Double)

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println(
        "Usage: MLPiplineApp <appname> <batchInterval> <hostname> <port>"
      )
      System.exit(1)
    }

    val Seq(appName, batchInterval, hostname, port) = args.toSeq
    val conf = new SparkConf().setAppName(appName).setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))
    val sqlC = new SQLContext(ssc.sparkContext)

    import sqlC.implicits._

    val substream = ssc.socketTextStream(hostname, port.toInt)
      .filter(!_.contains("NaN"))
      .map(_.split(" "))
      .filter(f => f(1) == "4" || f(1) == "5")
      .map(f => Array(f(1), f(4), f(5), f(6), f(20), f(21), f(22), f(36), f(37), f(38)))
      .map(f => f.map(v => v.toDouble))
      .foreachRDD( rdd => {
        if (!rdd.isEmpty()) {
          val accelermeter = rdd.map(x => Activity(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9))).toDF()
          val split = accelermeter.randomSplit(Array(0.3, 0.7))
          val test = split(0)
          val train = split(1)
          val assembler = new VectorAssembler()

        }
      })

    ssc.start()
    ssc.awaitTermination()

  }

}
