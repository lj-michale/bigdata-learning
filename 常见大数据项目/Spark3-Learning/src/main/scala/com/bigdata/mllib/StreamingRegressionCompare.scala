package com.bigdata.mllib

import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @descr 利用流应用来监控-流式机器学习Model
 *
 * @author lj.michale
 * @date 2021-06
 */
object StreamingRegressionCompare {

  val logger = Logger.getLogger(StreamingRegressionCompare.getClass)

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[2]", "StreamingRegressionExample001", Seconds(10))
    val stream = ssc.socketTextStream("192.168.10.128", 9999)

    val numFeatures = 100

    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0,0,0,0,0,0,0,0,0,0))
      .setNumIterations(1).setStepSize(0.01)

    // 竞争模型2，超参数不一样
    val model2 = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0,0,0,0,0,0,0,0,0,0))
      .setNumIterations(1).setStepSize(0.1)

    // 将是数据源处理成回归需要的格式
    val labeledStream:DStream[LabeledPoint] = stream.map(event => {
        val split = event.split("\t")
        val y = split(0).toDouble
        val feature = split(1).split(",").map(_.toDouble)
        LabeledPoint(label = y, features = Vectors.dense(feature))
    })

    model.trainOn(labeledStream)
    model2.trainOn(labeledStream)

    val predsAndTrue = labeledStream.transform({
      rdd => {
        val latest1 = model.latestModel()
        val latest2 = model2.latestModel()
        rdd.map( point => {
          val pred1 = latest1.predict(point.features)
          val pred2 = latest2.predict(point.features)
          (pred1 - point.label, pred2 - point.label)
        })
      }
    })

    // 计算均方误差与均方误差根
    predsAndTrue.foreachRDD((rdd, time) => {
      // 均方误差
      val mse1 = rdd.map{case(err1, err2) => err1 * err2}.mean()
      // 均方根误差
      val rmse1 = math.sqrt(mse1)
      val mse2 = rdd.map{case (err1, err2) => err1 * err2}.mean()
      val rmse2 = math.sqrt(mse2)
      println(s"Time：$time")
      println(s"MSE current batch: Model 1: $mse1; Model2: $mse2")
      println(s"RMSE current batch: Model 1: $rmse1; Model 2: $rmse2")
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
