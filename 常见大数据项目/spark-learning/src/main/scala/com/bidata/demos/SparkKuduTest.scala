package com.bidata.demos

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scala.collection.JavaConverters._

object SparkKuduTest {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkKuduTest").setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val kuduContext = new KuduContext("szgl-bdspvt-statstest01-1161.jpushoa.com:7051,szgl-bdspvt-statstest02-1157.jpushoa.com:7051,szgl-bdspvt-statstest03-1131.jpushoa.com:7051", sc)
    createTable(kuduContext)

    sc.stop();

  }

  /**
   * 创建表
   *
   * @param kuduContext
   * @return
   */
  def createTable(kuduContext: KuduContext) = {
    val tableName = "spark_kudu"
    val schema = StructType(
      StructField("userId", StringType, false) ::
        StructField("name", StringType, false) ::
        StructField("age", IntegerType, false) ::
        StructField("sex", StringType, false) :: Nil
    )

    val primaryKey = Seq("userId")
    val options = new CreateTableOptions

    options.setRangePartitionColumns(List("userId").asJava)
    options.setNumReplicas(1)

    if (!kuduContext.tableExists(tableName)) {
      kuduContext.createTable(tableName, schema, primaryKey, options)
    }
  }
}

