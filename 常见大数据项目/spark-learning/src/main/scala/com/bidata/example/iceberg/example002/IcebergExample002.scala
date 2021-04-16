package com.bidata.example.iceberg.example002

import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.{PartitionSpec, Schema, Table}
import org.apache.iceberg.types.Types
import org.apache.iceberg.hive.HiveCatalog


object IcebergExample002 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop") // 设置数据源类别为hadoop
      .config("spark.sql.catalog.hadoop_prod", classOf[SparkCatalog].getName)
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://linux01:8020//doit/iceberg/warehouse/") // 设置数据源位置
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    // 读Iceberg table, 通过DataFrame Spark 2.4只能读写已经存在的Iceberg table。在后续的操作前，需要先通过Iceberg API来创建table
    val name = TableIdentifier.of("default", "person")
    val schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.required(3, "age", Types.IntegerType.get()))
    val spec:PartitionSpec = PartitionSpec.unpartitioned
    val catalog:HiveCatalog = new HiveCatalog(spark.sparkContext.hadoopConfiguration)
    val table:Table = catalog.createTable(name, schema, spec)


    spark.close()

  }

}
