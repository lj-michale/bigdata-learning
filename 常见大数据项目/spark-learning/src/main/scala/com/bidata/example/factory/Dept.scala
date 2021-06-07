package com.bidata.example.factory

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

trait Dept extends Serializable {
  // attributes
  def config: Config
  def product_json: Map[String, List[Map[String, String]]]

  // methods
  def createTable(spark: SparkSession, btime: String, etime: String): Unit
}
