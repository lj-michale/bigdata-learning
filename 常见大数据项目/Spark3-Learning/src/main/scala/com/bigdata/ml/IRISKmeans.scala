package com.bigdata.ml

import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
 * @descr KMeans聚类计算
 *
 * @author lj.michale
 * @date 2021-06
 */
object IRISKmeans {

  val logger = Logger.getLogger(PipelineExample.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // Iris数据集数据结构
    val fields = Array("id","SepalLength","SepalWidth","PetalLength","PetalWidth","Species")

    val fieldTypes = fields.map(
      r => if (r == "id" || r == "Species")
        {StructField(r, StringType)}
        else
        {StructField(r, DoubleType)}
    )

    val schema = StructType(fieldTypes)
    val featureCols = Array("SepalLength","SepalWidth","PetalLength","PetalWidth")
    val data = spark.read.schema(schema).option("header", true).csv("file:///E:\\company\\myself\\datasets\\Iris.csv")
//    +---+-----------+----------+-----------+----------+-----------+
//    | id|SepalLength|SepalWidth|PetalLength|PetalWidth|    Species|
//    +---+-----------+----------+-----------+----------+-----------+
//    |  1|        5.1|       3.5|        1.4|       0.2|Iris-setosa|
//    |  2|        4.9|       3.0|        1.4|       0.2|Iris-setosa|
//    |  3|        4.7|       3.2|        1.3|       0.2|Iris-setosa|
//    |  4|        4.6|       3.1|        1.5|       0.2|Iris-setosa|
//    |  5|        5.0|       3.6|        1.4|       0.2|Iris-setosa|
//    |  6|        5.4|       3.9|        1.7|       0.4|Iris-setosa|

    val vectorAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val pca = new PCA().setInputCol("features")
      .setOutputCol("pcaFeatures")
      // 主成分个数
      .setK(2)

    val scaler = new StandardScaler().setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, pca, scaler))
    val model = pipeline.fit(data)

    val predictions = model.transform(data)

    val evaluator = new ClusteringEvaluator()
    val silhouetor = evaluator.evaluate(predictions)

    println(s"Silhouette with squared euclidean distance = $silhouetor")

    spark.close()

  }

}
