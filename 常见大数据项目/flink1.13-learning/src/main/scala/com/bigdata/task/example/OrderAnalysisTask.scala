package com.bigdata.task.example

import java.time.ZoneId
import java.util.Properties

import com.bigdata.bean.RawData
import com.luoj.source.GenerateCustomOrderSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * @author lj.michale
 * @description  Order Data Analysis
 * @date 2021-08-21
 */
object OrderAnalysisTask {

  def main(args: Array[String]): Unit = {

//    val logger:Logger = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {

      val paramTool = ParameterTool.fromArgs(args)

//      logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
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
      val orderStream = env.addSource(new GenerateCustomOrderSource)
      orderStream.print()
//      // Tuple7<String, String, String, String, String, Double, String>
//      val orderDS:DataStream[String] = orderStream.map(row => {
//        val oredrId:String = row.getField(0)
//        val customerId:String = row.getField(1)
//        val productId:String = row.getField(2)
//        val productName:String = row.getField(3)
//        val price:String = row.getField(4)
//        val buyMoney:Double = row.getField(5)
//        val buyCount:Int = (buyMoney / price.toDouble).toInt
//        val buyTime:String = row.getField(5)
//        val rawData = RawData(oredrId, customerId, productId, productName, price, buyMoney, buyCount, buyTime)
//        rawData
//      }).name("getBuyCount")
//        .map(row => {
//          row.oredrId + "|" + row.customerId + "|" + row.productId + "|" + row.productName + "|" + row.price + "|" + row.buyMoney + "|" + row.buyCount + "|" + row.buyTime
//        }).name("toStringDataStream")
//
//      // 将自定义Source产生的data转发到Kafka
//      val kafkaSink = new FlinkKafkaProducer[String]("mzpns", new SimpleStringSchema(), producerProp)
//      orderDS.addSink(kafkaSink)
//
//      // consumer data from kafka
//      val kafkaSource = new FlinkKafkaConsumer[String]("mzpns", new SimpleStringSchema(), consumerProp)
//      val kafkaDataStream:DataStream[String] = env.addSource(kafkaSource).setParallelism(1)
//      kafkaDataStream.print()


      env.execute(s"${this.getClass.getName}")

    }

  }

}
