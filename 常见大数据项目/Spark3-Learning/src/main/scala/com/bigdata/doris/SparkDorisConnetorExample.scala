package com.bigdata.doris

import org.apache.spark.sql.SparkSession

/**
 * 使用Spark 操作doris
 */
object SparkDorisConnetorExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkDorisConnetorExample")
      .master("local[*]")
      .getOrCreate()

    val dorisSparkDF = spark.read.format("doris")
//      .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
//      .option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
//      .option("user", "$YOUR_DORIS_USERNAME")
//      .option("password", "$YOUR_DORIS_PASSWORD")
      .option("doris.table.identifier", "turing.table1")
      .option("doris.fenodes", "p88-dataplat-master1:9030")
      .option("user", "root")
      .option("password", "")
      .load()

    dorisSparkDF.show(5)

    import org.apache.doris.spark._

    import spark.implicits._
//    ## batch sink
    val mockDataDF = List(
      (3, "440403001005", "21.cn"),
      (1, "4404030013005", "22.cn"),
      (33, null, "23.cn")
    ).toDF("id", "mi_code", "mi_name")
    mockDataDF.show(5)

    mockDataDF.write.format("doris")
      .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
      .option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
      .option("user", "$YOUR_DORIS_USERNAME")
      .option("password", "$YOUR_DORIS_PASSWORD")
      .save()

//    ## stream sink(StructuredStreaming)
    val kafkaSource = spark.readStream
      .option("kafka.bootstrap.servers", "$YOUR_KAFKA_SERVERS")
      .option("startingOffsets", "latest")
      .option("subscribe", "$YOUR_KAFKA_TOPICS")
      .format("kafka")
      .load()
    kafkaSource.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)")
      .writeStream
      .format("doris")
      .option("checkpointLocation", "$YOUR_CHECKPOINT_LOCATION")
      .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
      .option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
      .option("user", "$YOUR_DORIS_USERNAME")
      .option("password", "$YOUR_DORIS_PASSWORD")
      .start()
      .awaitTermination()

    spark.stop()

  }

}
