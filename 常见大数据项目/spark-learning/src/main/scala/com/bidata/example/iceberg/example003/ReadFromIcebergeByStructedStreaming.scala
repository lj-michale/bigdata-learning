package com.bidata.example.iceberg.example003

import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession

/**
 * @descr 写入Iceberge
 * @date 2021/6/7 11:35
 */
object ReadFromIcebergeByStructedStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod", classOf[SparkCatalog].getName)
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://linux01:8020//doit/iceberg/warehouse/")
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    // Read Data From Socket By StructedStreaming
    val lineDS = spark.readStream.format("socket").option("host", "linux01").option("port", 9999).load()

    // Data Deal
    import  spark.implicits._
    val dataDF = lineDS.map(row => row.getAs[String]("value")).map(s => {
      val split: Array[String] = s.split(",")
      (split(0).toInt,split(1),split(2).toInt)
    }).toDF("id", "name", "age")

    // 指定hadoop表位置
    val tableIdentifier: String = "hdfs://linux01:8020/doit/iceberg/warehouse/default/tb_user"

    // 将数据写入到hadoop类型的表中
    val query = dataDF.writeStream.outputMode("append").format("iceberg").option("path", tableIdentifier).option("checkpointLocation", "/").start()
    query.awaitTermination()
    spark.close()

  }

}
