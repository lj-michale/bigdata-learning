package com.bigdata.task.learn.flinksql

import java.time.ZoneOffset.ofHours
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author lj.michale
 * @description FlinkSQLExample001
 * @date 2021-05-20
 */
object FlinkSQLExample0002 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
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
    tableEnv.getConfig.setLocalTimeZone(ofHours(8))

    val kafkaProperties: Properties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProperties.setProperty("group.id", "event")
    kafkaProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val flinkKafkaConsumer:FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("mzpns", new SimpleStringSchema, kafkaProperties)

    import org.apache.flink.streaming.api.scala._
    val kafkaDS:DataStream[String] = env.addSource(flinkKafkaConsumer)
//    kafkaDS.print()

    // parse json
    val parseDS = kafkaDS.map(x => {
      val arr = x.split(",")
      (arr(0).toInt, arr(1), arr(2).toInt)
    })

    tableEnv.createTemporaryView("temporary_view", kafkaDS)

//    val table:Table = tableEnv.sqlQuery("select * from temporary_view")
//    tableEnv.toAppendStream[String](table).print()

    val table2 = tableEnv.fromDataStream(kafkaDS, $("funcName"), $("data"))
    val dsRow: DataStream[Row] = tableEnv.toAppendStream[Row](table2)
    dsRow.print()

    env.execute("FlinkSQLExample0002")

  }

}
