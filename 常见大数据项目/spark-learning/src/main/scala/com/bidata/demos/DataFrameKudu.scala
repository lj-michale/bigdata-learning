package com.bidata.demos

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.kudu.spark.kudu._
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

    // Kudu Native RDD
    //使用kuduContext对象调用kuduRDD方法，需要sparkContext对象，表名，想要的字段名称
    val kuduRDD: RDD[Row] = kuduContext.kuduRDD(sc,tableName,Seq("name","age"))

    //操作该rdd 打印输出
    val result: RDD[(String, Int)] = kuduRDD.map {
      case Row(name: String, age: Int) => (name, age)
    }

    result.foreach(println)

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

  /**
   * 删除表的数据
   * @param sparkSession
   * @param sc
   * @param kuduMaster
   * @param kuduContext
   * @param tableName
   */

  private def deleteData(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String, kuduContext: KuduContext, tableName: String): Unit = {
    //定义一个map集合，封装kudu的相关信息
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )

    import sparkSession.implicits._
    val data = List(People(1, "zhangsan", 20), People(2, "lisi", 30), People(3, "wangwu", 40))
    val dataFrame: DataFrame = sc.parallelize(data).toDF
    dataFrame.createTempView("temp")

    //获取年龄大于30的所有用户id
    val result: DataFrame = sparkSession.sql("select id from temp where age >30")
    //删除对应的数据，这里必须要是主键字段
    kuduContext.deleteRows(result, tableName)
  }

  /**
   * 更新数据--添加数据
   *
   * @param sc
   * @param kuduMaster
   * @param kuduContext
   * @param tableName
   */
  private def UpsertData(sparkSession: SparkSession,sc: SparkContext, kuduMaster: String, kuduContext: KuduContext, tableName: String): Unit = {
    //更新表中的数据
    //定义一个map集合，封装kudu的相关信息
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )

    import sparkSession.implicits._
    val data = List(People(1, "zhangsan", 50), People(5, "tom", 30))
    val dataFrame: DataFrame = sc.parallelize(data).toDF
    //如果存在就是更新，否则就是插入
    kuduContext.upsertRows(dataFrame, tableName)
  }

  /**
   * 更新数据
   * @param sparkSession
   * @param sc
   * @param kuduMaster
   * @param kuduContext
   * @param tableName
   */
  private def updateData(sparkSession: SparkSession,sc: SparkContext, kuduMaster: String, kuduContext: KuduContext, tableName: String): Unit = {
    //定义一个map集合，封装kudu的相关信息
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )

    import sparkSession.implicits._
    val data = List(People(1, "zhangsan", 60), People(6, "tom", 30))
    val dataFrame: DataFrame = sc.parallelize(data).toDF
    //如果存在就是更新，否则就是报错
    kuduContext.updateRows(dataFrame, tableName)

  }


  // DataFrameApi读取kudu表中的数据

  /**
   * 使用DataFrameApi读取kudu表中的数据
   * @param sparkSession
   * @param kuduMaster
   * @param tableName
   */
  private def getTableData(sparkSession: SparkSession, kuduMaster: String, tableName: String): Unit = {
    //定义map集合，封装kudu的master地址和要读取的表名
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )
    sparkSession.read.options(options).kudu.show()
  }

  // DataFrameApi写数据到kudu表中

  /**
   * DataFrame api 写数据到kudu表
   * @param sparkSession
   * @param sc
   * @param kuduMaster
   * @param tableName
   */
  private def dataFrame2kudu(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String, tableName: String): Unit = {

    //定义map集合，封装kudu的master地址和要读取的表名
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )

    val data = List(People(7, "jim", 30), People(8, "xiaoming", 40))
    import sparkSession.implicits._
    val dataFrame: DataFrame = sc.parallelize(data).toDF
    //把dataFrame结果写入到kudu表中 ,目前只支持append追加
    dataFrame.write.options(options).mode("append").kudu
    //查看结果
    //导包
    import org.apache.kudu.spark.kudu._
    //加载表的数据，导包调用kudu方法，转换为dataFrame，最后在使用show方法显示结果
    sparkSession.read.options(options).kudu.show()

  }

  // 使用sparksql操作kudu表
  /**
   * 使用sparksql操作kudu表
   * @param sparkSession
   * @param sc
   * @param kuduMaster
   * @param tableName
   */
  private def SparkSql2Kudu(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String, tableName: String): Unit = {
    //定义map集合，封装kudu的master地址和表名
    val options = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )
    val data = List(People(10, "小张", 30), People(11, "小王", 40))
    import sparkSession.implicits._
    val dataFrame: DataFrame = sc.parallelize(data).toDF
    //把dataFrame注册成一张表
    dataFrame.createTempView("temp1")
    //获取kudu表中的数据，然后注册成一张表
    sparkSession.read.options(options).kudu.createTempView("temp2")

    //使用sparkSQL的insert操作插入数据
    sparkSession.sql("insert into table temp2 select * from temp1")
    sparkSession.sql("select * from temp2 where age >30").show()
  }


}
