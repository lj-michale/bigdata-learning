package com.bidata.common

import java.io.{File, InputStreamReader}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.commons.io.IOUtils

object ConfigReader {

  def readFromDefault():Config = {
    ConfigFactory.load()
  }

  def readFromLocal(config_path: String): Config = {
    ConfigFactory.parseFile(new File(config_path))
  }

  def readFromHdfs(spark: SparkSession, path: String): Config ={
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfig)
    val file = fs.open(new Path(path))
    val reader = new InputStreamReader(file)
    var config: Config = null
    try {
      config = ConfigFactory.parseReader(reader)
    } catch {
      case e: Exception => println(s"Can't load data from HDFS: $e")
    } finally {
      reader.close()
    }
    config
  }

  def readJsonFromHdfs(spark: SparkSession, path: String): String = {
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConfig)
    val file = fs.open(new Path(path))
    val reader = new InputStreamReader(file)
    var json_str = ""
    try {
      json_str = IOUtils.toString(reader)
    } catch {
      case e: Exception => println{s"Can't load data from HDFS: $e"}
    } finally {
      reader.close()
    }
    json_str
  }
}

