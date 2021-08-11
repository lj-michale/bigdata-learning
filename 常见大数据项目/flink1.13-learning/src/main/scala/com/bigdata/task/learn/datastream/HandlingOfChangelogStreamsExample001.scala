package com.bigdata.task.learn.datastream

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.Schema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.{Row, RowKind}

/**
 * @description Converting between DataStream and Table
 *              Handling of Changelog Streams
 *  https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/data_stream_api/
 * @author lj.michale
 * @date 2021-07-16
 */
object HandlingOfChangelogStreamsExample001 {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    // create environments of both APIs
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
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

    /**
     * Internally, Flink’s table runtime is a changelog processor. The concepts page describes how dynamic tables and streams relate to each other.
     * A StreamTableEnvironment offers the following methods to expose these change data capture (CDC) functionalities:
     * fromChangelogStream(DataStream): Interprets a stream of changelog entries as a table. The stream record type must be org.apache.flink.types.
     *                                 Row since its RowKind flag is evaluated during runtime. Event-time and watermarks are not propagated by default.
     *                                 This method expects a changelog containing all kinds of changes (enumerated in org.apache.flink.types.RowKind) as the default ChangelogMode.
     * fromChangelogStream(DataStream, Schema): Allows to define a schema for the DataStream similar to fromDataStream(DataStream, Schema). Otherwise the semantics are equal to fromChangelogStream(DataStream).
     * fromChangelogStream(DataStream, Schema, ChangelogMode): Gives full control about how to interpret a stream as a changelog. The passed ChangelogMode helps the planner to distinguish between insert-only, upsert, or retract behavior.
     * toChangelogStream(Table): Reverse operation of fromChangelogStream(DataStream). It produces a stream with instances of org.apache.flink.types.Row and sets the RowKind flag for every record at runtime.
     *                           All kinds of updating tables are supported by this method. If the input table contains a single rowtime column, it will be propagated into a stream record’s timestamp. Watermarks will be propagated as well.
     * toChangelogStream(Table, Schema): Reverse operation of fromChangelogStream(DataStream, Schema). The method can enrich the produced column data types. The planner might insert implicit casts if necessary.
     *                                   It is possible to write out the rowtime as a metadata column.
     * toChangelogStream(Table, Schema, ChangelogMode): Gives full control about how to convert a table to a changelog stream. The passed ChangelogMode helps the planner to distinguish between insert-only, upsert, or retract behavior.
     */
    // === EXAMPLE 1 ===
//    // interpret the stream as a retract stream
//    // create a changelog DataStream
//    val dataStream = env.fromElements(
//      Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
//      Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
//      Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", Int.box(12)),
//      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
//    )(Types.ROW(Types.STRING, Types.INT))
//
//    // interpret the DataStream as a Table
//    val table = tableEnv.fromChangelogStream(dataStream)
//    // register the table under a name and perform an aggregation
//    tableEnv.createTemporaryView("InputTable", table)
//    tableEnv
//      .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
//      .print()
    // prints:
    // +----+--------------------------------+-------------+
    // | op |                           name |       score |
    // +----+--------------------------------+-------------+
    // | +I |                            Bob |           5 |
    // | +I |                          Alice |          12 |
    // | -D |                          Alice |          12 |
    // | +I |                          Alice |         100 |
    // +----+--------------------------------+-------------+

    // === EXAMPLE 2 ===
    // interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)
    // create a changelog DataStream
    val dataStream = env.fromElements(
      Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
      Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
    )(Types.ROW(Types.STRING, Types.INT))

    // interpret the DataStream as a Table
    val table =
      tableEnv.fromChangelogStream(
        dataStream,
        Schema.newBuilder().primaryKey("f0").build(),
        ChangelogMode.upsert())

    // register the table under a name and perform an aggregation
    tableEnv.createTemporaryView("InputTable", table)
    tableEnv
      .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
      .print()
    // prints:
    // +----+--------------------------------+-------------+
    // | op |                           name |       score |
    // +----+--------------------------------+-------------+
    // | +I |                            Bob |           5 |
    // | +I |                          Alice |          12 |
    // | -D |                          Alice |          12 |
    // | +I |                          Alice |         100 |
    // +----+--------------------------------+-------------+


    env.execute(s"${this.getClass.getName}")

  }

}
