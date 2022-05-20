package com.bigdata.pipeline.batch

import java.text.SimpleDateFormat
import java.util.Properties

import com.bigdata.common.utils.JdbcUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Retention {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("innerdemo").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val properties = new Properties()
    properties.setProperty("user",JdbcUtils.user)
    properties.setProperty("password",JdbcUtils.pwd)
    properties.setProperty("Driver",JdbcUtils.driver)

    val logs: DataFrame = spark.read.jdbc(JdbcUtils.url,JdbcUtils.table_full_access_logs,properties)
    logs.cache()
    //    logs.show(1,false)
    //    logs.printSchema()
    import spark.implicits._

    //拉取信息，ActionName 为Registered 的数据
    val registered: DataFrame = logs.filter($"actionName" === "Registered")
      .withColumnRenamed("event_time", "register_time")
      .select("userUID", "register_time")
    registered.printSchema()
    //    registered.show(3,false)

    //拉取信息，ActionName 为Signin 的数据
    val signin: DataFrame = logs.filter($"actionName" === "Signin")
      .withColumnRenamed("event_time", "signin_time")
      .select("userUID", "signin_time")
    //      signin.show(3,false)

    val joined: DataFrame = registered.join(signin,Seq("userUID"),"left")
    joined.printSchema()
    joined.show(3,false)

    val  spdf=new SimpleDateFormat("yyyy-MM-dd")
    //注册UDF  传入参数  字符串 输出 Long 类型  的数字
    val gszh: UserDefinedFunction = spark.udf.register("gszh", (event_time: String) => {
      if (StringUtils.isEmpty(event_time))
        0
      else {
        spdf.parse(event_time).getTime
      }
    })

    val joined2: DataFrame = joined.withColumn("register_date",gszh($"register_time"))
      .withColumn("signin_date",gszh($"signin_time"))
    //    joined2.show(3,false)
    //    joined2.printSchema()
    import org.apache.spark.sql.functions._
    //一天有  86400000 毫秒,统计出注册后次日登录的客户数量
    val result1: DataFrame = joined2.filter($"register_date" + 86400000 === $"signin_date")
      .groupBy($"register_date", $"userUID").agg(count("userUID"))
      .groupBy($"register_date").agg(count("userUID").as("num1"))
    //    result1.printSchema()
    //    result1.show()

    //按照时间，统计出当日注册总客户数
    registered.printSchema()
    val resultsum: DataFrame = joined2.groupBy("register_date").agg(countDistinct("userUID").as("num_sum"))

    //计算客户次日留存率    =   客户注册后次日登录数据  / 注册日的总数量  * 100%
    //    val Retention: Column = concat((result1.toString().toInt/resultsum.toString().toInt)*100.toString,"%")
    result1.join(resultsum,"register_date").select($"num1"/$"num_sum"*100).show()

  }
}
