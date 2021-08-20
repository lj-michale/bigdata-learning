package com.bigdata.ml

import org.apache.log4j.Logger
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector

object PipelineExample {

  val logger = Logger.getLogger(PipelineExample.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PipelineExample")
      .master("local[*]")
//      .enableHiveSupport()
      .getOrCreate()

    // 准备DataSet,最后以列表示标签：判断是否为垃圾邮件
    val traning = spark.createDataFrame(
      Seq((0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0))).toDF("id","text","label")

    // Pipeline
    val tokennizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokennizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)

    val pipeline = new Pipeline().setStages(Array(tokennizer, hashingTF, lr))

    // 拟合模型
    val model = pipeline.fit(traning)

    // 将模型持久化
    model.write.overwrite().save("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\Spark3-Learning\\model")

    // 将Pipeline持久化
    pipeline.write.overwrite().save("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\Spark3-Learning\\pipeline")

    // 加载模型
    val sameModel = PipelineModel.load("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\Spark3-Learning\\model")

    // 准备无标签测试数据集
    val test = spark.createDataFrame(Seq((4L, "Spark i j k"),
      (5L, "l n m"),
      (6L, "Spark hadoop spark"),
      (7L, "apache hadoop"))).toDF("id", "text")

    // 使用模型预测测试数据集，得到预测结果(标签)
    model.transform(test).select("id", "text", "probability", "prediction").collect().foreach({
      case Row(id:Long, text:String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) ----> prob=$prob, prediction=$prediction")
    })

  }

}
