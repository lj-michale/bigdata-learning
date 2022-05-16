package com.bigdata.doris

import org.apache.spark.sql.SparkSession

object SparkDorisConnetorExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkDorisConnetorExample")
      .master("local[*]")
      .getOrCreate()

    val dorisSparkDF = spark.read.format("doris")
//      .option("doris.table.identifier", "$YOUR_DORIS_DATABASE_NAME.$YOUR_DORIS_TABLE_NAME")
//      .option("doris.fenodes", "$YOUR_DORIS_FE_HOSTNAME:$YOUR_DORIS_FE_RESFUL_PORT")
//      .option("user", "$YOUR_DORIS_USERNAME")
//      .option("password", "$YOUR_DORIS_PASSWORD")
      .option("doris.table.identifier", "turing.table1")
      .option("doris.fenodes", "p88-dataplat-master1:9030")
      .option("user", "root")
      .option("password", "")
      .load()

    dorisSparkDF.show(5)

    spark.stop()

  }

}
