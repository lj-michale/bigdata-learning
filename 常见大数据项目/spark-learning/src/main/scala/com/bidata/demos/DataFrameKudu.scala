package com.bidata.demos

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scala.collection.JavaConverters._

case class People(id:Int,name:String,age:Int)

object DataFrameKudu {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("DataFrameKudu").setMaster("local[2]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("warn")
    val kuduMaster="szgl-bdspvt-statstest01-1161.jpushoa.com:7051,szgl-bdspvt-statstest02-1157.jpushoa.com:7051,szgl-bdspvt-statstest03-1131.jpushoa.com:7051"
    val kuduContext = new KuduContext(kuduMaster,sc)

    //定义表名
    val tableName="people"
    createTable(kuduContext, tableName)

    insertData2table(sparkSession,sc, kuduContext, tableName)

  }

  /**
   * 创建表
   * @param kuduContext
   * @param tableName
   */
  private def createTable(kuduContext: KuduContext, tableName: String): Unit = {
    val schema = StructType(
      StructField("id", IntegerType, false) ::
        StructField("name", StringType, false) ::
        StructField("age", IntegerType, false) :: Nil
    )

    //定义表的主键
    val tablePrimaryKey = List("id")

    //定义表的选项配置
    val options = new CreateTableOptions
    options.setRangePartitionColumns(List("id").asJava)
    options.setNumReplicas(1)

    //创建表
    if (!kuduContext.tableExists(tableName)) {
      kuduContext.createTable(tableName, schema, tablePrimaryKey, options)
    }
  }

  /**
   * 插入数据到表中
   * @param sparkSession
   * @param sc
   * @param kuduContext
   * @param tableName
   */
  private def insertData2table(sparkSession:SparkSession,sc: SparkContext, kuduContext: KuduContext, tableName: String): Unit = {
    val data = List(People(1, "zhangsan", 20), People(2, "lisi", 30), People(3, "wangwu", 40))
    val peopleRDD: RDD[People] = sc.parallelize(data)
    import sparkSession.implicits._
    val peopleDF: DataFrame = peopleRDD.toDF
    kuduContext.insertRows(peopleDF, tableName)
  }

}
