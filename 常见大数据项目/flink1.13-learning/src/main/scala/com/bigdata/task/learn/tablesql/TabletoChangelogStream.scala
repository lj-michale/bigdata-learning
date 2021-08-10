package com.bigdata.task.learn.tablesql

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import java.time.Instant

object TabletoChangelogStream {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
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

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    // create Table with event-time
    tableEnv.executeSql(
      """
      CREATE TABLE GeneratedTable (
        name STRING,
        score INT,
        event_time TIMESTAMP_LTZ(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
      )
      WITH ('connector'='datagen')
      """
    )

    val table = tableEnv.from("GeneratedTable")

    // === EXAMPLE 1 ===
    import org.apache.flink.table.api._
    // convert to DataStream in the simplest and most general way possible (no event-time)
    val simpleTable = tableEnv
      .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
      .as("name", "score")
      .groupBy($"name")
      .select($"name", $"score".sum())

    tableEnv
      .toChangelogStream(simpleTable)
      .executeAndCollect()
      .foreach(println)

    // === EXAMPLE 2 ===
    // convert to DataStream in the simplest and most general way possible (with event-time)
    val dataStream: DataStream[Row] = tableEnv.toChangelogStream(table)
//    dataStream.print()
    // since `event_time` is a single time attribute in the schema, it is set as the
    // stream record's timestamp by default; however, at the same time, it remains part of the Row
    dataStream.process(new ProcessFunction[Row, Unit] {
      override def processElement(
                                   row: Row,
                                   ctx: ProcessFunction[Row, Unit]#Context,
                                   out: Collector[Unit]): Unit = {
        // prints: [name, score, event_time]
        println(row.getFieldNames(true))
        // timestamp exists twice
        assert(ctx.timestamp() == row.getFieldAs[Instant]("event_time").toEpochMilli)
      }
    })

    // === EXAMPLE 3 ===

    // convert to DataStream but write out the time attribute as a metadata column which means
    // it is not part of the physical schema anymore

    val dataStream3: DataStream[Row] = tableEnv.toChangelogStream(
      table,
      Schema.newBuilder()
        .column("name", "STRING")
        .column("score", "INT")
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build())

    // the stream record's timestamp is defined by the metadata; it is not part of the Row
    dataStream3.process(new ProcessFunction[Row, Unit] {
      override def processElement(
                                   row: Row,
                                   ctx: ProcessFunction[Row, Unit]#Context,
                                   out: Collector[Unit]): Unit = {
        // prints: [name, score]
        println(row.getFieldNames(true))
        // timestamp exists once
        println(ctx.timestamp())
      }
    })
    dataStream3.print()

    // === EXAMPLE 4 ===

    // for advanced users, it is also possible to use more internal data structures for better
    // efficiency

    // note that this is only mentioned here for completeness because using internal data structures
    // adds complexity and additional type handling

    // however, converting a TIMESTAMP_LTZ column to `Long` or STRING to `byte[]` might be convenient,
    // also structured types can be represented as `Row` if needed

//    val dataStream: DataStream[Row] = tableEnv.toChangelogStream(
//      table,
//      Schema.newBuilder()
//        .column(
//          "name",
//          DataTypes.STRING().bridgedTo(classOf[StringData]))
//        .column(
//          "score",
//          DataTypes.INT())
//        .column(
//          "event_time",
//          DataTypes.TIMESTAMP_LTZ(3).bridgedTo(class[Long]))
//    .build())


    env.execute("TabletoChangelogStream")

  }

  case class StringData(name: StringData, score: Integer, event_time: Long)
}
