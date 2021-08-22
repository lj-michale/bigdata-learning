package com.bigdata.metric

import java.lang
import java.time.{Duration, ZoneId}
import java.util.{Properties}

import com.aurora.mq.kafka.CustomerKafkaConsumer
import com.aurora.source.GenerateCustomOrderSource
import com.bigdata.bean.RawData
import com.bigdata.common.ParseDeserialization
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}

import scala.collection.JavaConversions._
import scala.util.Random
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.bigdata.window.evictor.MyEvictor
import com.bigdata.window.trigger.CustomTrigger
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * @author lj.michale
 * @description  Flink自定义metric监控流入量
 * @date 2021-08-21
 */
object FlinkMetricDemo001 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

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

    import org.apache.flink.api.scala._
    val orderStreaam = env.addSource(new GenerateCustomOrderSource)

    // DataStream[Tuple7[String, String, String, String, String, Double, String]] -> DataStream[String]
    val orderDS:DataStream[String] = orderStreaam.map(row => {
        row.f0 + "," + row.f1 + "," + row.f2 + "," + row.f3 + "," + row.f4 + "," + row.f5.toString + "," + row.f6
    }).name("getOrderDS")
      .map(row => {
          val array = row.split(",")
          val oredrId:String = array(0)
          val customerId:String = array(1)
          val productId:String = array(2)
          val productName:String = array(3)
          val price:String = array(4)
          val buyMoney:Double = array(5).toDouble
          val buyCount:Int = (buyMoney / price.toDouble).toInt
          val buyTime:String = array(6)
          // println(s" >>>>>>> ${oredrId}, ${customerId}, ${productId}, ${productName}, ${price}, ${buyMoney}, ${buyCount}, ${buyTime}")
          val rawData = RawData(oredrId, customerId, productId, productName, price, buyMoney, buyCount, buyTime)
          rawData
      }).name("addBuyCount")
        .map(row => {
             row.oredrId + "|" + row.customerId + "|" + row.productId + "|" + row.productName + "|" + row.price + "|" + row.buyMoney + "|" + row.buyCount + "|" + row.buyTime
        }).name("toStringDataStream")

    // 将自定义Source产生的data转发到Kafka
    orderDS.addSink(new FlinkKafkaProducer[String](
      "mzpns",
      // 使用先前实现的消息序列化类
      new MyKafkaSerializationSchema("mzpns"),
      producerProp, Semantic.EXACTLY_ONCE
    )).name("sinkToKafka")

    // 自定义metric监控流入量
    val consumerSource:CustomerKafkaConsumer[RawData] = new CustomerKafkaConsumer[RawData]("mzpns", new ParseDeserialization, consumerProp)
    val kafkaStreaam:DataStream[RawData] = env.addSource(consumerSource)

    // 时间水印生成策略，WatermarkStrategy
    val watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness[RawData](Duration.ofSeconds(1))
      // 延迟1秒
      .withTimestampAssigner(new SerializableTimestampAssigner[RawData] {
        override def extractTimestamp(element: RawData, recordTimestamp: Long): Long = {
          // 指定事件时间字段
          element.buyTime.toLong * 1000L
        }
    })

    val outputTag = new OutputTag[RawData]("late-data") {}

    /**
     * 进行窗口计算，设置水位线、延迟数据容忍值，旁路输出以及各种窗口内，外(自定)计算函数
     */
    kafkaStreaam
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .keyBy(_.oredrId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
      .trigger(new CustomTrigger(10, 1 * 60 * 1000L))
      .evictor(new MyEvictor())
      .allowedLateness(Time.seconds(5))
      .sideOutputLateData(outputTag)

    kafkaStreaam.print()

    env.execute(s"${this.getClass.getName}")

  }

  /**
   * @descr 自定义KafkaSource,生产模拟data,并将数据转发到kafka
   */
  class MyCusRawDataSource() extends SourceFunction[RawData] {
    var isRunning = true

    override def run(sourceContext: SourceFunction.SourceContext[RawData]): Unit = {
      val rand = new Random()
      //产生无限流数据
      while(isRunning) {
//        //产生时间戳
//        val curTime:Long = Calendar.getInstance().getTimeInMillis
//        //发送出去
//        mapTemp.foreach(t => sContext.collect(SensorReading(t._1,curTime,t._2)))
//        //每隔100ms发送一条传感器数据
//        Thread.sleep(100)
      }
    }

    override def cancel(): Unit = {}

  }

  /**
   * @descr 实现消息序列化类，实现其中的serialize方法。设message是String类型。
   */
  class MyKafkaSerializationSchema(topic: String) extends KafkaSerializationSchema[String] {
    /**
     * @descr 序列化
     * @param element
     */
    override def serialize(element: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
//      logger.info("被序列化之前的数据:{}", element)
      new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes)
    }
  }


  /**
   * val schema = new RecordKafkaSchema
   * val kafkaConsumer = new FlinkKafkaConsumer010[ConsumerRecord[String, String]]("sgz_bi_player3_2", schema, kafka)
   */
  class RecordKafkaSchema extends KafkaDeserializationSchema[ConsumerRecord[String, String]] {
    override def isEndOfStream(nextElement: ConsumerRecord[String, String]): Boolean = false
    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): ConsumerRecord[String, String] = {
      var key: String = null
      var value: String = null

      if (record.key != null) {
        key = new String(record.key())
      }

      if (record.value != null) {
        value = new String(record.value())
      }

      new ConsumerRecord[String, String](
        record.topic(),
        record.partition(),
        record.offset(),
        record.timestamp(),
        record.timestampType(),
        record.checksum,
        record.serializedKeySize,
        record.serializedValueSize(),
        key,
        value)
    }

    override def getProducedType: TypeInformation[ConsumerRecord[String, String]] = TypeInformation.of(new TypeHint[ConsumerRecord[String, String]] {})
  }

}
