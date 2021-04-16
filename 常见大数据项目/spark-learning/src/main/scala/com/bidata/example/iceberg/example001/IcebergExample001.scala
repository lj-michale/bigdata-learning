package com.bidata.example.iceberg.example001

import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession

object IcebergExample001 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
//      .config("spark.sql.catalog.hadoop_prod.type", "hadoop") // 设置数据源类别为hadoop
//      .config("spark.sql.catalog.hadoop_prod", classOf[SparkCatalog].getName)
//      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://linux01:8020//doit/iceberg/warehouse/") // 设置数据源位置
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    // 获取表结构信息
    val df = spark.table("hadoop_prod.default.tb_test1")
    df.printSchema()

    // 读取指定表下的数据
    //spark.read.format("iceberg").load("/doit/iceberg/warehouse/default/tb_test1").show()

    // 读取指定快照下的数据
    spark.read.option("snapshot-id", 3372567346381641315l).format("iceberg").load("/doit/iceberg/warehouse/default/tb_test1").show

    spark.close()

  }

}
