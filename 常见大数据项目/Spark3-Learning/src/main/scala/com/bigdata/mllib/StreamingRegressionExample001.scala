package com.bigdata.mllib

import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @descr 流式机器学习
 * @author lj.michale
 * @date 2021-06
 */
object StreamingRegressionExample001 {

  val logger = Logger.getLogger(StreamingRegressionExample001.getClass)

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[2]", "StreamingRegressionExample001", Seconds(10))
    val stream = ssc.socketTextStream("192.168.10.128", 9999)

    val numFeatures = 100

    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(0,0,0,0,0,0,0,0,0,0))
      .setNumIterations(1).setStepSize(0.01)

    // 将是数据源处理成回归需要的格式
    val labeledStream:DStream[LabeledPoint] = stream.map(event => {
      val split = event.split("\t")
      val y = split(0).toDouble
      val feature = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(feature))
    })

    // 开始训练
    model.trainOn(labeledStream)

    // 此时还可以同时对另一个流进行预测
    //model.predictOn(labeledStream)

    ssc.start()
    ssc.awaitTermination()

  }

}
