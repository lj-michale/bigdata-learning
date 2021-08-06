package com.bigdata.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
 * @descr Catalog API
 *        park中的DataSet和Dataframe API支持结构化分析。结构化分析的一个重要的方面是管理元数据。
 *        这些元数据可能是一些临时元数据（比如临时表）、SQLContext上注册的UDF以及持久化的元数据（比如Hivemeta store或者HCatalog）。
 *        Spark的早期版本是没有标准的API来访问这些元数据的。
 *        用户通常使用查询语句（比如show tables）来查询这些元数据。
 *        这些查询通常需要操作原始的字符串，而且不同元数据类型的操作也是不一样的。
 *        这种情况在Spark 2.0中得到改变。Spark 2.0中添加了标准的API（称为catalog）来访问Spark SQL中的元数据。
 *        这个API既可以操作Spark SQL，也可以操作Hive元数据。
 * @author lj.michale
 * @date 2021-06
 */
object SparkCatalogExample {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("SparkSQLExample01")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val catalog = spark.catalog

    val inputPath = "file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\Spark3-Learning\\dataset\\iris.csv"
    val schema = new StructType(Array(
      StructField("sepal length", DoubleType, true),
      StructField("sepal width", DoubleType, true),
      StructField("petal length", DoubleType, true),
      StructField("petal width", DoubleType, true),
      StructField("class", StringType, true)))
    val rawInputDF = spark.read.schema(schema).csv(inputPath)
    rawInputDF.createTempView("temp")

    // 我们一旦创建好catalog对象之后，我们可以使用它来查询元数据中的数据库，catalog上的API返回的结果全部都是dataset。
    catalog.listDatabases().show(false)

    // 查询表
    // 正如我们可以展示出元数据中的所有数据库一样，我们也可以展示出元数据中某个数据库中的表。它会展示出Spark SQL中所有注册的临时表。同时可以展示出Hive中默认数据库（也就是default）中的表。
//    catalog.listTables().select("report").show(false)

    // 判断某个表是否缓存
    // 我们可以使用Catalog提供的API来检查某个表是否缓存

    // 查询已经注册的函数
    // 我们不仅可以使用Catalog API操作表，还可以用它操作UDF。下面代码片段展示SparkSession上所有已经注册号的函数，当然也包括了Spark内置的函数。
    catalog.listFunctions().select("name","className","isTemporary").show(100, false)


    spark.stop()

  }

}
