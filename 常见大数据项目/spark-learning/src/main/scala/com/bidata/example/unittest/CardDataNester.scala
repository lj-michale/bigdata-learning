//package com.bidata.example.unittest
//
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.functions._
//
//object CardDataNester {
//
//  def main(args:Array[String]): Unit = {
//    if (args.length == 0) {
//      println("<runLocal> " +
//        "<accountTable> " +
//        "<cardTable> " +
//        "<transTable> " +
//        "<nestedTableName> ")
//      return
//    }
//
//    val runLocal = args(0).equalsIgnoreCase("l")
//    val accountTable = args(1)
//    val cardTable = args(2)
//    val transTable = args(3)
//    val nestedTableName = args(4)
//
//    val spark: SparkSession = SparkSession
//      .builder
//      .master("local")
//      .appName("UserAnalysis")
//      .enableHiveSupport()      //启用hive
//      .getOrCreate()
//
//    //  引入隐式转换
//    import spark.implicits._
//    import org.apache.spark.sql.functions._
//
//    val transTableDF:DataFrame = spark.sql("select * from " + transTable)
//
//    val transGroupByRDD = transTableDF.map(r => {
//      (r.getLong(r.fieldIndex("card_id")), r)
//    })
//
//    val cardTableDF = spark.sql("select * from " + cardTable)
//
//    val nestedCardRDD = cardTableDF.map(r => {
//      (r.getLong(r.fieldIndex("card_id")), r)
//    }).join(transGroupByRDD).map(r => {
//      val card = r._2._1
//      val trans = r._2._2.map(t => {
//        Row(
//          t.getLong(t.fieldIndex("tran_id")),
//          t.getLong(t.fieldIndex("time_stamp")),
//          t.getInt(t.fieldIndex("amount")),
//          t.getLong(t.fieldIndex("merchant_id")))
//      })
//
//      (card.getLong(card.fieldIndex("account_id")),
//        Row(
//          card.getLong(card.fieldIndex("card_id")),
//          card.getInt(card.fieldIndex("exp_year")),
//          card.getInt(card.fieldIndex("exp_month")),
//          trans))
//    }).groupByKey()
//
//    val accountTableDF = hc.sql("select * from " + accountTable)
//
//    val nestedAccountRdd = accountTableDF.map(r => {
//      (r.getLong(r.fieldIndex("account_id")), r)
//    }).join(nestedCardRDD).map(r => {
//      val account = r._2._1
//      Row(
//        account.getLong(account.fieldIndex("account_id")),
//        account.getString(account.fieldIndex("first_name")),
//        account.getString(account.fieldIndex("last_name")),
//        account.getInt(account.fieldIndex("age")),
//        r._2._2.toSeq
//      )
//    })
//
//    spark.sql("create table " + nestedTableName + "(" +
//      " account_id BIGINT," +
//      " first_name STRING," +
//      " last_name STRING," +
//      " age INT," +
//      " card ARRAY<STRUCT<" +
//      "   card_id: BIGINT," +
//      "   exp_year: INT," +
//      "   exp_month: INT," +
//      "   tran: ARRAY<STRUCT<" +
//      "     tran_id: BIGINT," +
//      "     time_stamp: BIGINT," +
//      "     amount: INT," +
//      "     merchant_id: BIGINT" +
//      "   >>" +
//      " >>" +
//      ") stored as parquet")
//
//    val emptyNestedDf = spark.sql("select * from " + nestedTableName + " limit 0")
//
//    spark.createDataFrame(nestedAccountRdd, emptyNestedDf.schema).registerTempTable("nestedTmp")
//
//    spark.sql("insert into " + nestedTableName + " select * from nestedTmp")
//
//    spark.stop()
//  }
//}