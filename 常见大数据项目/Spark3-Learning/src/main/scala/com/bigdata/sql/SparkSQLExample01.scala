package com.bigdata.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.bigdata.sql.udf.AuroaUDF._

/**
 * @descr SparkSQLExample01
 * @author lj.michale
 * @date 2021-06
 */
object SparkSQLExample01 {

  val logger = Logger.getLogger(SparkSQLExample01.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSQLExample01")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    /**
     * 注册UDF函数
     */
    spark.udf.register("getChannel", getChannel _ )

    // SparkSQL
    spark.sql("show databases").show()
    spark.sql("use report")
    spark.sql("show tables").show()
    spark.sql("select * from houshuai_test").show()

    /**
     * @descr 通过隐式转换，转成DataSet
     */
    import spark.implicits._
    val personDS:Dataset[Person] = Seq(Person("Andy", 35)).toDS()
    personDS.show()

    spark.stop()

  }

  case class Person(name: String, age: Long)

}
