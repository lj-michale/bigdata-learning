package com.bigdata.task.planner

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Map, Properties}

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.flink.streaming.connectors.kafka.internals.{KafkaTopicPartition, KeyedSerializationSchemaWrapper}
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * @descr ./bin/flink run -m yarn-cluster -ynm ryj -c vip.shuai7boy.flink.checkpoint.TestSavepoints /data/flinkdata/MyFlinkObj-1.0-SNAPSHOT-jar-with-dependencies.jar
 *       查看kafka消息队列的积压情况命令
 *
 * @date 2021/4/13 13:45
 */
object BlinkPlannerExample {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val bsEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    bsEnv.setParallelism(1)

    val bsSettings:EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv:StreamTableEnvironment = StreamTableEnvironment.create(bsEnv, bsSettings)

    val props = new Properties()
    props.setProperty("bootstrap.servers","172.17.11.31:9092, 172.17.11.30:9092")
    props.setProperty("group.id","consumer-group")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    // 方式一
    import org.apache.flink.streaming.api.scala._
    val dStream: DataStream[String] = bsEnv.addSource(new FlinkKafkaConsumer011[String]("jiguang_001", new SimpleStringSchema(), props))
    // View 和 Table 的 Schema 完全相同。事实上，在 Table API 中，可以认为 View 和 Table是等价的。
    bsTableEnv.createTemporaryView("t_sensor_log",dStream)

    // 方式二
//    val consumerStream: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("jiguang_001", new SimpleStringSchema(), props)
//    val specificStartOffsets: util.Map[KafkaTopicPartition, java.lang.Long] = new util.HashMap[KafkaTopicPartition, java.lang.Long]()
//    specificStartOffsets.put(new KafkaTopicPartition("jiguang_001", 0), 23L)        // 第一个分区从23L开始
//    specificStartOffsets.put(new KafkaTopicPartition("jiguang_001", 1), 31L)        // 第二个分区从31L开始
//    specificStartOffsets.put(new KafkaTopicPartition("jiguang_001", 2), 43L)        // 第三个分区从43L开始
//    consumerStream.setStartFromSpecificOffsets(specificStartOffsets)
//    val dStream2: DataStream[String] = bsEnv.addSource(consumerStream)
//    dStream2.print()

    // 方式三: flink的Table API 与 SQL-外部连接器, https://blog.csdn.net/wwwzydcom/article/details/103749927
//    val schema = new Schema()
//      .field("code", DataTypes.STRING())
//      .field("total_emp", DataTypes.INT())
//      .field("ts", DataTypes.BIGINT())
//
//    bsTableEnv.connect(new Kafka()
//      .version("0.11")
//      .topic("mzpns")
//      .property("bootstrap.servers","172.17.11.31:9092, 172.17.11.30:9092")
//      .property("group.id","consumer-group")
//      .property("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//      .property("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//      .startFromEarliest()
//      .sinkPartitionerRoundRobin())
//      .withFormat(new Csv()).withSchema(schema).createTemporaryTable("t_sensor_log")
//
//    bsTableEnv.sqlQuery(
//      """
//        |SELECT code,total_emp,ts FROM t_sensor_log
//        |""".stripMargin)
//
//    val query:Table = bsTableEnv.sqlQuery(
//      """
//        |SELECT code FROM t_sensor_log
//      """.stripMargin)
//    bsTableEnv.toAppendStream[String](query).print()

    ///////////////////////////////////////////// DataStream  ///////////////////////////////////////////
    val sensorDStream: DataStream[SensorReading] = dStream.map( line => {
        val jsonData = JSON.parseObject(line)
        val code:String = jsonData.getString("code")
        val totalEmp:String = jsonData.getString("total_emp")
        val ts:Long = jsonData.getLong("ts")
        SensorReading(code, totalEmp, ts)
    })

    // 生成周期型策略生成水印
    val waterMarkStream = sensorDStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(SensorReading)] {
      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L  // 最大允许的乱序时间是10s
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

      override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        val timestamp = element.ts
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        val id = Thread.currentThread().getId
        println("code:{}", element.code, "totalEmp:{}", element.totalEmp, "ts:{}", sdf.format(element.ts), "currentMaxTimestamp:{}", currentMaxTimestamp, "currentMaxTimestamp:{}",currentMaxTimestamp, "watermark:{}",getCurrentWatermark().getTimestamp)
        timestamp
      }

    })

    // window 计算
//    val windowDStream:DataStream[String] = waterMarkStream.map( x => SensorReading(x.code, x.totalEmp, x.ts))
//      .timeWindowAll(Time.seconds(1),Time.seconds(1))
//      .sum(1).map(x => "ts:" + x.ts.toString + "totalEmp:" + x.totalEmp.toString )

//    val windowDStream:DataStream[SensorReading] = waterMarkStream.map( x => SensorReading(x.code, x.totalEmp, x.ts))
//      .timeWindowAll(Time.seconds(1),Time.seconds(1)).sum()

    // sink to Kafka
    val topic2 = "mzpns"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","192.168.200.10:9092")
    //设置事务超时时间
    prop.setProperty("transaction.timeout.ms", 60000 * 15 + "")
//    val producer = new FlinkKafkaProducer011[String](topic2,new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), prop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)
//    val producer = new FlinkKafkaProducer011[String](topic2, new SimpleStringSchema(), prop)
//    windowDStream.addSink(producer)
    waterMarkStream.print()


    ///////////////////////////////////////////// TableApi  /////////////////////////////////////////////



    ///////////////////////////////////////////// TableSQL  /////////////////////////////////////////////


    bsEnv.execute(s"${this.getClass.getSimpleName}")

  }

  def tranTimeToString(timestamp:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = fm.format(new Date(timestamp.toLong))
    time
  }

  case class SensorReading(code: String, totalEmp: String, ts: Long)

}
