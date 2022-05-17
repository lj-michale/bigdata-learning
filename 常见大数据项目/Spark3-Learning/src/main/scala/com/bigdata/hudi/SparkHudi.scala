//package com.bigdata.hudi
//
//import java.time.LocalDateTime
//
//import org.apache.log4j.Logger
//import org.apache.spark.sql.catalyst.encoders.RowEncoder
//import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.streaming.StreamingQueryListener
//import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
//import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
//
//object SparkHudi {
//  val logger = Logger.getLogger(SparkHudi.getClass)
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession
//      .builder
//      .appName("SparkHudi")
//      //.master("local[*]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.default.parallelism", 9)
//      .config("spark.sql.shuffle.partitions", 9)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    // 添加监听器，每一批次处理完成，将该批次的相关信息，如起始offset，抓取记录数量，处理时间打印到控制台
//    spark.streams.addListener(new StreamingQueryListener() {
//      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
//        println("Query started: " + queryStarted.id)
//      }
//      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
//        println("Query terminated: " + queryTerminated.id)
//      }
//      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
//        println("Query made progress: " + queryProgress.progress)
//      }
//    })
//
//    // 定义kafka流
//    val dataStreamReader = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "testTopic")
//      .option("startingOffsets", "latest")
//      .option("maxOffsetsPerTrigger", 100000)
//      .option("failOnDataLoss", false)
//
//    // 加载流数据，这里因为只是测试使用，直接读取kafka消息而不做其他处理，是spark结构化流会自动生成每一套消息对应的kafka元数据，如消息所在主题，分区，消息对应offset等。
//    val df = dataStreamReader.load()
//      .selectExpr(
//        "topic as kafka_topic"
//    "CAST(partition AS STRING) kafka_partition",
//    "cast(timestamp as String) kafka_timestamp",
//    "CAST(offset AS STRING) kafka_offset",
//    "CAST(key AS STRING) kafka_key",
//    "CAST(value AS STRING) kafka_value",
//    "current_timestamp() current_time",
//    )
//    .selectExpr(
//      "kafka_topic"
//    "concat(kafka_partition,'-',kafka_offset) kafka_partition_offset",
//    "kafka_offset",
//    "kafka_timestamp",
//    "kafka_key",
//    "kafka_value",
//    "substr(current_time,1,10) partition_date")
//
//    // 创建并启动query
//    val query = df
//      .writeStream
//      .queryName("demo").
//    .foreachBatch { (batchDF: DataFrame, _: Long) => {
//      batchDF.persist()
//
//      println(LocalDateTime.now() + "start writing cow table")
//      batchDF.write.format("org.apache.hudi")
//        .option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
//        .option(PRECOMBINE_FIELD_OPT_KEY, "kafka_timestamp")
//        // 以kafka分区和偏移量作为组合主键
//        .option(RECORDKEY_FIELD_OPT_KEY, "kafka_partition_offset")
//        // 以当前日期作为分区
//        .option(PARTITIONPATH_FIELD_OPT_KEY, "partition_date")
//        .option(TABLE_NAME, "copy_on_write_table")
//        .option(HIVE_STYLE_PARTITIONING_OPT_KEY, true)
//        .mode(SaveMode.Append)
//        .save("/tmp/sparkHudi/COPY_ON_WRITE")
//
//      println(LocalDateTime.now() + "start writing mor table")
//      batchDF.write.format("org.apache.hudi")
//        .option(TABLE_TYPE_OPT_KEY, "MERGE_ON_READ")
//        .option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
//        .option(PRECOMBINE_FIELD_OPT_KEY, "kafka_timestamp")
//        .option(RECORDKEY_FIELD_OPT_KEY, "kafka_partition_offset")
//        .option(PARTITIONPATH_FIELD_OPT_KEY, "partition_date")
//        .option(TABLE_NAME, "merge_on_read_table")
//        .option(HIVE_STYLE_PARTITIONING_OPT_KEY, true)
//        .mode(SaveMode.Append)
//        .save("/tmp/sparkHudi/MERGE_ON_READ")
//
//      println(LocalDateTime.now() + "finish")
//      batchDF.unpersist()
//    }
//    }
//      .option("checkpointLocation", "/tmp/sparkHudi/checkpoint/")
//      .start()
//
//    query.awaitTermination()
//  }
//}
