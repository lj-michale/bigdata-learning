package com.bigdata.connector.oracle

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-05-12
 */
object Grade {

  Class.forName("oracle.jdbc.driver.OracleDriver")

  val connection: Connection = DriverManager.getConnection("sqlUrl", "user", "password")
  // SQL请换成你自己的
  val prepareStatement: PreparedStatement = connection
    .prepareStatement("""insert into GIS_SCORERECORD(ID,......) values(SEQ_GIS_SCORERECORD.nextval,?.....)""")
  var num = 0

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).master("local[*]").appName("Grade").getOrCreate()

    spark.sql("""你的SQL""").rdd.collect.foreach {
      rows => {
        prepareStatement.setBigDecimal(1, rows.getDecimal(0))
        prepareStatement.setBigDecimal(2, rows.getDecimal(1))
        prepareStatement.setLong(3, rows.getLong(2)) //
        prepareStatement.setBigDecimal(4, rows.getDecimal(3))
        prepareStatement.setBigDecimal(5, rows.getDecimal(4)) //
        prepareStatement.setBigDecimal(6, rows.getDecimal(5))
        prepareStatement.setTimestamp(7, rows.getTimestamp(6))
        prepareStatement.addBatch()
        num += 1
        if (num % 500 == 0) {
          prepareStatement.executeBatch()
        }
     }
    }

    prepareStatement.executeBatch()
    prepareStatement.close()
    connection.close()

    spark.stop()

  }

}
