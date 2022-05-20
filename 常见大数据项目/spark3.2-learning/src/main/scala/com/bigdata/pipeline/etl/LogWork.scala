package com.bigdata.pipeline.etl

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object LogWork extends App{

  val spark = SparkSession.builder().master("local[*]").appName("LogWork")
    .config("spark.ececutor.memory","4G")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._
  private val logfileRDD: RDD[Array[String]] = sc
    .textFile("E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark3.2-learning\\dataset\\test.log")
    .map(_.split("\t"))
  //logfileRDD.take(1)
  private val filterRDD: RDD[Array[String]] = logfileRDD.filter(_.length==8)
  private val rowRDD: RDD[Row] = filterRDD.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))

  private val logschema = StructType(Array(
    StructField("event_time", StringType, true),
    StructField("url", StringType, true),
    StructField("method", StringType, true),
    StructField("status", StringType, true),
    StructField("sip", StringType, true),
    StructField("user_uip", StringType, true),
    StructField("action_prepend", StringType, true)
  ))

  private val logDF: DataFrame = spark.createDataFrame(rowRDD,logschema)
  private val fileterLogs: Dataset[Row] = logDF.dropDuplicates("event_time", "url")
    .filter(x => x(3) == "200")
    .filter(x => StringUtils.isNotEmpty(x(0).toString))
  //  fileterLogs.printSchema()
  //  fileterLogs.show()
  //  将url按照”&”以及”=”切割
  private val full_access_log_rdd: RDD[Row] = fileterLogs.map(line => {
    val paramsArr = line.getAs[String]("url").split("\\?")
    var paraMap = Map("params" -> "")
    if (paramsArr.length == 2) {
      paraMap = paramsArr(1).split("&").map(_.split("="))
        .filter(_.length == 2)
        .map(x => (x(0), x(1)))
        .toMap
    }
    (
      line.getAs[String]("event_time"),
      paraMap.getOrElse[String]("userUID", ""),
      paraMap.getOrElse[String]("userSID", ""),
      paraMap.getOrElse[String]("userUIP", ""),
      paraMap.getOrElse[String]("actionBegin", ""),
      paraMap.getOrElse[String]("actionEnd", ""),
      paraMap.getOrElse[String]("actionType", ""),
      paraMap.getOrElse[String]("actionName", ""),
      paraMap.getOrElse[String]("actionValue", ""),
      paraMap.getOrElse[String]("actionTest", ""),
      paraMap.getOrElse[String]("ifEquipment", ""),
      line.getAs[String]("method"),
      line.getAs[String]("status"),
      line.getAs[String]("sip"),
      line.getAs[String]("user_uip"),
      line.getAs[String]("action_prepend")
    )
  }).toDF().rdd

  private val full_access_log_schema = StructType(Array(
    StructField("event_time", StringType, true),
    StructField("user_uid", StringType, true),
    StructField("user_sid", StringType, true),
    StructField("user_uip", StringType, true),
    StructField("action_begin", StringType, true),
    StructField("action_end", StringType, true),
    StructField("action_type", StringType, true),
    StructField("action_name", StringType, true),
    StructField("action_value", StringType, true),
    StructField("action_test", StringType, true),
    StructField("if_equipment", StringType, true),
    StructField("method", StringType, true),
    StructField("status", StringType, true),
    StructField("sip", StringType, true),
    StructField("user_uip", StringType, true),
    StructField("action_prepend", StringType, true)
  ))

  private val full_access_log_DF: DataFrame = spark.createDataFrame(full_access_log_rdd,full_access_log_schema)
  full_access_log_DF.printSchema()
  full_access_log_DF.show()

  sc.stop()
}
