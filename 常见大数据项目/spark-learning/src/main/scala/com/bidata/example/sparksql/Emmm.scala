package com.bidata.example.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Emmm {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(this.getClass.getSimpleName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(
      Class.forName("scala.collection.mutable.WrappedArray$ofRef"),
      Class.forName("org.apache.spark.sql.types.StringType$"),
      classOf[TPerson],
      classOf[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema],
      classOf[org.apache.spark.sql.types.StructType],
      classOf[org.apache.spark.sql.types.StructField],
      classOf[org.apache.spark.sql.types.Metadata],
      classOf[Array[TPerson]],
      classOf[Array[org.apache.spark.sql.Row]],
      classOf[Array[org.apache.spark.sql.types.StructField]],
      classOf[Array[Object]]
    ))
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    // 使用样例类创建RDD并转化成DF后又回到RDD
    spark.sparkContext.parallelize(Seq(TPerson("zs", "21"), TPerson("ls", "25"))).toDF().rdd
      .map(row => {
        // 打印schema
        println(row.schema)
        // 得到Row中的数据并往其中添加我们要新增的字段值
        val buffer = Row.unapplySeq(row).get.map(_.asInstanceOf[String]).toBuffer
        buffer.append("男") //增加一个性别
        buffer.append("北京") //增肌一个地址

        // 获取原来row中的schema,并在原来Row中的Schema上增加我们要增加的字段名以及类型.
        val schema: StructType = row.schema
          .add("gender", StringType)
          .add("address", StringType)
        // 使用Row的子类GenericRowWithSchema创建新的Row
        val newRow: Row = new GenericRowWithSchema(buffer.toArray, schema)
        // 使用新的Row替换成原来的Row
        newRow
      }).map(row => {
      // 打印新的schema
      println(row.schema)
      // 测试我们新增的字段
      val gender = row.getAs[String]("gender")
      // 获取原本就有的字段
      val name = row.getAs[String]("name")
      val age = row.getAs[String]("age")
      // 获取新的字段
      val address = row.getAs[String]("address")
      // 输出查看结果
      println(s"$name-$age-$gender-$address")
      row
    }).collect()
    spark.stop()
  }

  /**
   * 样例类
   *
   * @param name name属性
   * @param age  age属性
   */
  case class TPerson(name: String, age: String)

}
