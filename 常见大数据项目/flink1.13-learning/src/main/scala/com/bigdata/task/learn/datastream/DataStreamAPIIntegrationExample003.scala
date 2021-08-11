package com.bigdata.task.learn.datastream

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import java.time.Instant

// imports for Scala DataStream API
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._

// imports for Table API with bridging to Scala DataStream API
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * @description Converting between DataStream and Table
 *  https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/data_stream_api/
 * @author lj.michale
 * @date 2021-07-16
 */
object DataStreamAPIIntegrationExample003 {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    // create environments of both APIs
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))
    // 状态后端-HashMapStateBackend
    env.setStateBackend(new HashMapStateBackend)
    //等价于MemoryStateBackend
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    // start defining your pipelines in both APIs...
    import org.apache.flink.api.scala._
    // create a DataStream
    val dataStream = env.fromElements(
      User("Alice", 4, Instant.ofEpochMilli(1000)),
      User("Bob", 6, Instant.ofEpochMilli(1001)),
      User("Alice", 10, Instant.ofEpochMilli(1002)))

    // === EXAMPLE 1 ===
//    // derive all physical columns automatically
//    val table = tableEnv.fromDataStream(dataStream)
//    table.printSchema()
//    // prints:
//    // (
//    //  `name` STRING,
//    //  `score` INT,
//    //  `event_time` TIMESTAMP_LTZ(9)
//    // )

    // === EXAMPLE 2 ===
//    // derive all physical columns automatically
//    // but add computed columns (in this case for creating a proctime attribute column)
//    val table = tableEnv.fromDataStream(
//      dataStream,
//      Schema.newBuilder()
//        .columnByExpression("proc_time", "PROCTIME()")
//        .build())
//    table.printSchema()
//    // prints:
//    // (
//    //  `name` STRING,
//    //  `score` INT NOT NULL,
//    //  `event_time` TIMESTAMP_LTZ(9),
//    //  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
//    //)

//    // === EXAMPLE 3 ===
//    // derive all physical columns automatically
//    // but add computed columns (in this case for creating a rowtime attribute column)
//    // and a custom watermark strategy
//    val table =
//      tableEnv.fromDataStream(
//        dataStream,
//        Schema.newBuilder()
//          .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
//          .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
//          .build())
//    table.printSchema()
//    // prints:
//    // (
//    //  `name` STRING,
//    //  `score` INT,
//    //  `event_time` TIMESTAMP_LTZ(9),
//    //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
//    //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
//    // )

    // === EXAMPLE 4 ===
//    // derive all physical columns automatically
//    // but access the stream record's timestamp for creating a rowtime attribute column
//    // also rely on the watermarks generated in the DataStream API
//    // we assume that a watermark strategy has been defined for `dataStream` before
//    // (not part of this example)
//    val table =
//    tableEnv.fromDataStream(
//      dataStream,
//      Schema.newBuilder()
//        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
//        .watermark("rowtime", "SOURCE_WATERMARK()")
//        .build())
//    table.printSchema()
//    // prints:
//    // (
//    //  `name` STRING,
//    //  `score` INT,
//    //  `event_time` TIMESTAMP_LTZ(9),
//    //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
//    //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
//    // )

    // === EXAMPLE 5 ===
    // define physical columns manually
    // in this example,
    //   - we can reduce the default precision of timestamps from 9 to 3
    //   - we also project the columns and put `event_time` to the beginning
    val table =
      tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
          .column("event_time", "TIMESTAMP_LTZ(3)")
          .column("name", "STRING")
          .column("score", "INT")
          .watermark("event_time", "SOURCE_WATERMARK()")
          .build())
    table.printSchema()
    // prints:
    // (
    //  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
    //  `name` VARCHAR(200),
    //  `score` INT
    // )
    // note: the watermark strategy is not shown due to the inserted column reordering projection


    env.execute("DataStreamAPIIntegrationExample003")

  }

  // some example case class
  case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)
}
