package com.bigdata.task.learn.datastream

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.DataTypes
import org.apache.flink.types.Row

/**
 * @description Converting between DataStream and Table
 *              Examples for toDataStream
 *  https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/data_stream_api/
 * @author lj.michale
 * @date 2021-07-16
 */
object DataStreamAPIIntegrationExample005 {

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

    tableEnv.executeSql(
      """
      CREATE TABLE GeneratedTable (
        name STRING,
        score INT,
        event_time TIMESTAMP_LTZ(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
      )
      WITH ('connector'='datagen')
      """)

    val table = tableEnv.from("GeneratedTable")

    // === EXAMPLE 1 ===
    // use the default conversion to instances of Row
    // since `event_time` is a single rowtime attribute, it is inserted into the DataStream
    // metadata and watermarks are propagated
    val dataStream: DataStream[Row] = tableEnv.toDataStream(table)
//    dataStream.print()

    // === EXAMPLE 2 ===
    // a data type is extracted from class `User`,
    // the planner reorders fields and inserts implicit casts where possible to convert internal
    // data structures to the desired structured type
    // since `event_time` is a single rowtime attribute, it is inserted into the DataStream
    // metadata and watermarks are propagated
    val dataStream2: DataStream[User] = tableEnv.toDataStream(table, classOf[User])
    dataStream2.print()

    // data types can be extracted reflectively as above or explicitly defined
    val dataStream3: DataStream[User] =
      tableEnv.toDataStream(
        table,
        DataTypes.STRUCTURED(
          classOf[User],
          DataTypes.FIELD("name", DataTypes.STRING()),
          DataTypes.FIELD("score", DataTypes.INT()),
          DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))))
//    dataStream3.print()

    env.execute("DataStreamAPIIntegrationExample005")

  }

  case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)

}
