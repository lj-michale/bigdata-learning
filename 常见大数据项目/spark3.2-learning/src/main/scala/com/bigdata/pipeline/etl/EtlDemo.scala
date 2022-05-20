package com.bigdata.pipeline.etl

import java.util.Properties

import com.bigdata.common.utils.JdbcUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
 * @descr 日志清洗Spark-ETL示例
 *
 * @author lj.michale
 * @date 2022-05-20
 */
object EtlDemo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("innerdemo").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._
    // 加载日志数据，按照\t 分割，过滤出长度为8的数据，将数据封装到Row中
    val rowRDD: RDD[Row] = sc.textFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark3.2-learning\\dataset\\test.log")
      .map(x => x.split("\t"))
      .filter(x => x.length >= 8)
      .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))
    // rowRDD.collect().foreach(println)

    // 创建Schema
    val logs_schema = StructType(
      Array(
        StructField("event_time", StringType),
        StructField("url", StringType),
        StructField("method", StringType),
        StructField("status", StringType),
        StructField("sip", StringType),
        StructField("user_uip", StringType),
        StructField("action_prepend", StringType),
        StructField("action_client", StringType)
      )
    )
    val logDF: DataFrame = spark.createDataFrame(rowRDD,logs_schema)
    //    logDF.printSchema()
    //    logDF.show(3)

    //去除重复的event_time,url 列，过滤出状态为200 的数据，去除event_time 为空的数据
    val filterLogs: Dataset[Row] = logDF.dropDuplicates("event_time", "url")
      .filter(x => x(3) == "200")
      .filter(x => StringUtils.isNotEmpty(x(0).toString))

    val full_logs_rdd: RDD[Row] = filterLogs.map(line => {
      val str: String = line.getAs[String]("url")
      val paramsArray: Array[String] = str.split("\\?")
      var paramsMap: Map[String, String] = null
      if (paramsArray.length == 2) {
        paramsMap = paramsArray(1)
          .split("&")
          .map(x => x.split("="))
          .filter(x => x.length == 2)
          .map(x => (x(0), x(1))).toMap
      }
      (
        line.getAs[String]("event_time"),
        paramsMap.getOrElse[String]("userUID", ""),
        paramsMap.getOrElse[String]("userSID", ""),
        paramsMap.getOrElse[String]("actionBegin", ""),
        paramsMap.getOrElse[String]("actionEnd", ""),
        paramsMap.getOrElse[String]("actionType", ""),
        paramsMap.getOrElse[String]("actionName", ""),
        paramsMap.getOrElse[String]("actionValue", ""),
        paramsMap.getOrElse[String]("actionTest", ""),
        paramsMap.getOrElse[String]("ifEquipment", ""),
        line.getAs[String]("method"),
        line.getAs[String]("status"),
        line.getAs[String]("sip"),
        line.getAs[String]("user_uip"),
        line.getAs[String]("action_prepend"),
        line.getAs[String]("action_client")

      )
    }).toDF().rdd

    val full_logs_schema =StructType(
      Array(
        StructField("event_time", StringType),
        StructField("userUID", StringType),
        StructField("userSID", StringType),
        StructField("actionBegin", StringType),
        StructField("actionEnd", StringType),
        StructField("actionType", StringType),
        StructField("actionName", StringType),
        StructField("actionValue", StringType),
        StructField("actionTest", StringType),
        StructField("ifEquipment", StringType),
        StructField("method", StringType),
        StructField("status", StringType),
        StructField("sip", StringType),
        StructField("user_uip", StringType),
        StructField("action_prepend", StringType),
        StructField("action_client", StringType)
      )
    )

    val full_logDF: DataFrame = spark.createDataFrame(full_logs_rdd,full_logs_schema)
    full_logDF.show(3,false)
    full_logDF.printSchema()

    //连接Mysql数据库
    val properties = new Properties()
    properties.setProperty("user",JdbcUtils.user)
    properties.setProperty("password",JdbcUtils.pwd)
    properties.setProperty("Driver",JdbcUtils.driver)

    println("写入filerLogs 到myusql数据库")
    filterLogs.write.mode(SaveMode.Overwrite).jdbc(JdbcUtils.url,JdbcUtils.table_access_logs,properties)
    println("写入 filterLogs 到数据库完成")

    println("写入filer_LogDF 到myusql数据库")
    full_logDF.write.mode(SaveMode.Overwrite).jdbc(JdbcUtils.url,JdbcUtils.table_full_access_logs,properties)
    println("写入 filter_LogDF  到数据库完成")

    spark.stop()


  }
}
