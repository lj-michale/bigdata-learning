package com.bidata.example.iceberg.example002

import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object IcebergExample002_02 {

  def main(args: Array[String]): Unit = {

    val sparkConf:SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark:SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val catalog:HiveCatalog = new HiveCatalog(spark.sparkContext.hadoopConfiguration)
    val data = Seq((1, "a"), (2, "b"), (3, "c"))


  }

}
