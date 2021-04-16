package com.bidata.example.avro

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Date

object learning1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("readFileFromFaFq").setMaster("local")
    val sc = new SparkContext(conf)
    // import needed for the .avro method to be added
    import com.databricks.spark.avro._

    val sqlContext = new SQLContext(sc)

    // The Avro records get converted to Spark types, filtered, and
    // then written back out as Avro records
    val df = sqlContext.read.avro("file/data/avro/input/episodes.avro")
    df.show

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    df.filter("doctor > 5").write.avro("file/data/avro/output/episodes/avro" + iString)
    df.filter("doctor > 5").show

  }

}
