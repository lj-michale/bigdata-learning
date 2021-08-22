package com.bigdata.theme

import java.time.{Duration, ZoneId}
import java.util.Properties

import com.aurora.source.GenerateCustomOrderSource
import com.bigdata.bean.RawData
import com.bigdata.window.evictor.MyEvictor
import com.bigdata.window.trigger.CustomTrigger
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

object OrderAnalysisTask2 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val paramTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")
    env.setStateBackend(new EmbeddedRocksDBStateBackend)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    val consumerProp = new Properties()
    consumerProp.setProperty("bootstrap.servers", "localhost:9092")
    consumerProp.setProperty("group.id", "event")
    consumerProp.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    consumerProp.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producerProp = new Properties()
    producerProp.setProperty("bootstrap.servers", "localhost:9092")
    // 设置flink的producer的transaction.timeout.ms，flink默认为1h，大于kafka server允许的15m，会报错，所以需要修改为15m以内的时间
    producerProp.setProperty("transaction.timeout.ms", s"${1000 * 60 * 5}")
    producerProp.setProperty("auto.offset.reset", "earliest")
    producerProp.setProperty("group.id", "t_feature_source")
    producerProp.setProperty("enable.auto.commit", "true")
    producerProp.setProperty("auto.commit.interval.ms", "1000")
    producerProp.setProperty("session.timeout.ms", "30000")
    producerProp.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    producerProp.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    import org.apache.flink.api.scala._
    val orderStream = env.addSource(new GenerateCustomOrderSource)
    val orderDS:DataStream[String] = orderStream.map(row => {
      val oredrId:String =  row.f0
      val customerId:String = row.f1
      val productId:String =  row.f2
      val productName:String =  row.f3
      val price:String =  row.f4
      val buyMoney:Double =  row.f5.toDouble
      val buyCount:Int = (buyMoney / price.toDouble).toInt
      val buyTime:String =  row.f6
      val rawData = RawData(oredrId, customerId, productId, productName, price, buyMoney, buyCount, buyTime)
      rawData
    }).name("getBuyCount")
      .map(row => {
        row.oredrId + "," + row.customerId + "," + row.productId + "," + row.productName + "," + row.price + "," + row.buyMoney + "," + row.buyCount + "," + row.buyTime
      }).name("toStringDataStream")

    // 将自定义Source产生的data转发到Kafka
    val kafkaSink = new FlinkKafkaProducer[String]("mzpns", new SimpleStringSchema(), producerProp)
    orderDS.addSink(kafkaSink)

    // consumer data from kafka
    val kafkaSource = new FlinkKafkaConsumer[String]("mzpns", new SimpleStringSchema(), consumerProp)
    val kafkaDataStream:DataStream[String] = env.addSource(kafkaSource).setParallelism(1)
    // O-20210822190730-86615,C-00007,P-00006,狮子头,98.35,295.05,3,2021-08-22 19:07:30
    val orderDataStream = kafkaDataStream
      .filter(!_.contains("|"))
      .map(row => {
        val array = row.toString.split(",")
        val oredrId:String =  array(0)
        val customerId:String = array(1)
        val productId:String =  array(2)
        val productName:String =  array(3)
        val price:String =  array(4)
        val buyMoney:Double =  array(5).toDouble
        val buyCount:Int = array(6).toInt
        val buyTime:String =  array(7)
        val rawData = RawData(oredrId, customerId, productId, productName, price, buyMoney, buyCount, buyTime)
        rawData
    }).name("getOrderDataFromKafka")

    // 时间水印生成策略，WatermarkStrategy
    val watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness[RawData](Duration.ofSeconds(1))
      // 延迟1秒
      .withTimestampAssigner(new SerializableTimestampAssigner[RawData] {
        override def extractTimestamp(element: RawData, recordTimestamp: Long): Long = {
          // 指定事件时间字段
          com.aurora.common.DataUtil.parseStringToLong(element.buyTime) * 1000L
        }
      })

    val outputTag = new OutputTag[RawData]("late-data") {}

    // 进行窗口计算
    val windowDataStream = orderDataStream.assignTimestampsAndWatermarks(watermarkStrategy)
      .keyBy(_.oredrId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
      // 用增量ReduceFunction与WindowFunction组合以返回窗口中的最小事件以及窗口的开始时间
      .reduce(
        (r1: RawData, r2: RawData) => { if (r1.buyMoney > r2.buyMoney) r2 else r1 },
        ( key: String,
          window: TimeWindow,
          minReadings: Iterable[RawData],
          out: Collector[(Long, RawData)] ) => {
          val min = minReadings.iterator.next()
          out.collect((window.getStart, min))})
      //.trigger(new CustomTrigger(10, 1 * 60 * 1000L))
      //.evictor(new MyEvictor())
      //.allowedLateness(Time.seconds(5))
      //.sideOutputLateData(outputTag)
    windowDataStream.print()

    env.execute(s"${this.getClass.getName}")

  }

//  class MyProcessWindowFunction extends ProcessWindowFunction[RawData, RawData, String, TimeWindow] {
//    override def process(key: String, context: Context, elements: Iterable[RawData], out: Collector[RawData]): Unit = {
//      out.collect(elements)
//    }
//  }

}
