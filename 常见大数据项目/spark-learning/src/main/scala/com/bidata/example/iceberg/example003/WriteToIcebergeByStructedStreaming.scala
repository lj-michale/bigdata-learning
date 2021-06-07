package com.bidata.example.iceberg.example003

import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @descr 读取Iceberge
 * @date 2021/6/7 11:35
 */
object WriteToIcebergeByStructedStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod", classOf[SparkCatalog].getName)
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://linux01:8020//doit/iceberg/warehouse/")
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val lines = spark.read.format("iceberg").load("hdfs://linux01:8020/doit/iceberg/warehouse/default/tb_user")
    lines.printSchema()
    lines.createTempView("tb_user")

    // 展示表所有的文件和所有的快照信息
    spark.sql("select * from hadoop_prod.default.tb_user.files").show()
    spark.sql("select * from hadoop_prod.default.tb_user.snapshots").show()

    // 查询指定快照的数据
    val lines2= spark.read.format("iceberg").option("snapshot-id", 9146975902480919479L).load("hdfs://linux01:8020/doit/iceberg/warehouse/default/tb_user")
    lines2.show()
    lines.show()

    spark.close()

  }

}
