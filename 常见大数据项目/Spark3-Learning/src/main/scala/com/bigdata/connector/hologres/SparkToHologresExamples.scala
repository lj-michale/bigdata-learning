package com.bigdata.connector.hologres

import com.alibaba.hologres.spark2.sink.SourceProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @descri SparkToHologresExamples 示例
 *
 * @author lj.michale
 * @date 2022-05-12
 */
object SparkToHologresExamples {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("DauApp")
      .setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Hologres兼容PostgreSQL，因为Spark可以用读取PostgreSQL的方式读取Hologres中的数据
    val readDf = spark.read
      // 使用postgresql jdbc driver读取holo
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url", "jdbc:postgresql://hgprecn-cn-7pp2nrn1p004-cn-shenzhen.hologres.aliyuncs.com:80/memberpro")
      .option("dbtable", "hologres_statistic.hg_table_statistic")
      .option("user", "BASIC$mbrproo")
      .option("password", "CTG_memberpro@20222")
      .load()

    readDf.show(200)

//    readDf.write
//      .format("hologres")
//      // 阿里云账号的AccessKey ID
//      .option(SourceProvider.USERNAME, "AccessKey ID")
//      // 阿里云账号的Accesskey SECRET
//      .option(SourceProvider.PASSWORD, "Accesskey SECRET")
//      // Hologres的Ip和Port
//      .option(SourceProvider.ENDPOINT, "Ip:Port")
//      .option(SourceProvider.DATABASE, "Database")
//      .option(SourceProvider.TABLE, "Table")
//      .save()

    spark.stop()

  }

}
