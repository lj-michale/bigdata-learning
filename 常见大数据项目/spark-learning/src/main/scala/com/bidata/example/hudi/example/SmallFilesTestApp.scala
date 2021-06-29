package com.bidata.example.hudi.example

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
 * Hudi小文件测试
 */
object SmallFilesTestApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val userStrDataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", false)
      .option("numPartitions", 5)
      .load()

    // 解析JSON串
    val userDF = userStrDataFrame.as[String]
      .map(User(_))
      // 添加分区字段
      .withColumn("dt", $"createTime".substr(0, 10))

    userDF.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .option("numRows", 100)
      .start()

    // 写入到Hudi
    val hudiTableName = "hudi_t_user_mor";
    val hiveDatabaseName = "hudi_datalake"
    val hiveTableName = "hudi_ods_user_mor"

    userDF.writeStream
      .outputMode(OutputMode.Append())
      .format("hudi")
      .option("hoodie.table.name", hudiTableName)
      .option("hoodie.bootstrap.base.path", "/hudi")
      .option("hoodie.datasource.write.table.name", hudiTableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.precombine.field", "dt")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "dt")
      .option("hoodie.datasource.write.hive_style_partitioning", false)
      .option("hoodie.datasource.hive_sync.enable", true)
      .option("hoodie.datasource.hive_sync.database", hiveDatabaseName)
      .option("hoodie.datasource.hive_sync.table", hiveTableName)
      .option("hoodie.datasource.hive_sync.username", "hive")
      .option("hoodie.datasource.hive_sync.password", "hive")
      .option("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://ha-node1:10000")
      .option("hoodie.datasource.hive_sync.partition_extractor_class", "cn.pin.streaming.tools.DayPartitionValueExtractor")
      .option("hoodie.datasource.hive_sync.partition_fields", "dt")
      .option("hoodie.datasource.hive_sync.use_jdbc", true)
      .option("hoodie.datasource.hive_sync.auto_create_database", true)
      .option("hoodie.datasource.hive_sync.skip_ro_suffix", false)
      .option("hoodie.datasource.hive_sync.support_timestamp", false)
      .option("hoodie.bootstrap.parallelism", 5)
      .option("hoodie.bulkinsert.shuffle.parallelism", 5)
      .option("hoodie.insert.shuffle.parallelism", 5)
      .option("hoodie.delete.shuffle.parallelism", 5)
      .option("hoodie.upsert.shuffle.parallelism", 5)
      .start(s"/hudi/${hudiTableName}")
      .awaitTermination()
  }
}