package com.bidata.example.sparksql

import java.util.Properties
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 演示SparkSQL读取各种数据源的数据，进行分析
 */
object SchemaTsvSpark {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SchemaTsvSpark")
      .master("local[2]")
      //设置sparkSQL中shuffle 时分区数
      .config("spark.sql.shuffle.partitions",2)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //自定义Schema信息
    val schema:StructType = StructType(
      Array(
        StructField("user_id",IntegerType,true),
        StructField("item_id",IntegerType,true),
        StructField("rating",DoubleType,true),
        StructField("timestamp",LongType,true)
      )
    )

    val mlRatingDF: DataFrame = spark.read
      .option("sep","\t")
      .schema(schema)
      .csv("file:///E:/u.data")


    mlRatingDF.printSchema()
    mlRatingDF.show(4,false)

    spark.read
      .option("sep",",")
      .option("header","true") //获取字段的名称
      .option("inferSchema","true") //自动推断出各列的数据类型
      .csv("file:///E:/ml-100/u_data.csv")
      .printSchema()

    spark.stop()
  }
}


