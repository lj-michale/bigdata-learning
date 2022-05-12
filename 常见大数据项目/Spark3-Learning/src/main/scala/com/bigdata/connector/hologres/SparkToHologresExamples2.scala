package com.bigdata.connector.hologres

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._
import com.alibaba.hologres.spark2.sink.SourceProvider

/**
 * @descri SparkToHologresExamples 示例
 *
 *         CREATE TABLE tb008 (
 *         id BIGINT primary key,
 *         counts INT,
 *         name TEXT,
 *         price NUMERIC(38, 18),
 *         out_of_stock BOOL,
 *         weight DOUBLE PRECISION,
 *         thick FLOAT,
 *         time TIMESTAMPTZ,
 *         dt DATE,
 *         by bytea,
 *         inta int4[],
 *         longa int8[],
 *         floata float4[],
 *         doublea float8[],
 *         boola boolean[],
 *         stringa text[]
 *         );
 *
 *         SELECT * FROM "public".tb008
 *
 *
 * @author lj.michale
 * @date 2022-05-12
 */
object SparkToHologresExamples2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("SparkToHologresExamples2")
      .setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val byteArray = Array(1.toByte, 2.toByte, 3.toByte, 'b'.toByte, 'a'.toByte)
    val intArray = Array(1, 2, 3)
    val longArray = Array(1L, 2L, 3L)
    val floatArray = Array(1.2F, 2.44F, 3.77F)
    val doubleArray = Array(1.222, 2.333, 3.444)
    val booleanArray = Array(true, false, false)
    val stringArray = Array("abcd", "bcde", "defg")

    val data = Seq(
      Row(-7L, 100, "phone1", BigDecimal(1234.567891234), false, 199.35, 6.7F, Timestamp.valueOf("2021-01-01 00:00:00"), Date.valueOf("2021-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray),
      Row(6L, -10, "phone2", BigDecimal(1234.56), true, 188.45, 7.8F, Timestamp.valueOf("2021-01-01 00:00:00"), Date.valueOf("1970-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray),
      Row(1L, 10, "phone3\"", BigDecimal(1234.56), true, 111.45, null, Timestamp.valueOf("2020-02-29 00:12:33"), Date.valueOf("2020-07-23"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray)
    )

    val schema = StructType(Array(
      StructField("id", LongType),
      StructField("counts", IntegerType),
      StructField("name", StringType, false), //false表示此Field不允许为null
      StructField("price", DecimalType(38, 12)),
      StructField("out_of_stock", BooleanType),
      StructField("weight", DoubleType),
      StructField("thick", FloatType),
      StructField("time", TimestampType),
      StructField("dt", DateType),
      StructField("by", BinaryType),
      StructField("inta", ArrayType(IntegerType)),
      StructField("longa", ArrayType(LongType)),
      StructField("floata", ArrayType(FloatType)),
      StructField("doublea", ArrayType(DoubleType)),
      StructField("boola", ArrayType(BooleanType)),
      StructField("stringa", ArrayType(StringType))
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    df.show()

    //配置导入数据至Hologres的信息。
    df.write.format("hologres") //必须配置为hologres
      .option(SourceProvider.USERNAME, "BASIC$mbrpro") //阿里云账号的AccessKey ID。
      .option(SourceProvider.PASSWORD, "CTG_memberpro@2022") //阿里云账号的Accesskey SECRET。
      .option(SourceProvider.ENDPOINT, "hgprecn-cn-7pp2nrn1p004-cn-shenzhen.hologres.aliyuncs.com:80")  // Hologres的Ip和Port。
      .option(SourceProvider.DATABASE, "memberpro") //Hologres的数据库名称,示例为test_database。
      .option(SourceProvider.TABLE, "public.tb008") //Hologres用于接收数据的表名称，示例为tb008。
      .option(SourceProvider.WRITE_BATCH_SIZE, 512) // 写入攒批大小
//      .option(SourceProvider.INPUT_DATA_SCHEMA_DDL, df.schema.toDDL) // Dataframe对应的DDL，仅spark3.x需要
      .mode(SaveMode.Append) // 仅spark3.x需要
      .save()

    spark.stop()

  }

}
