package com.bigdata.feature.table

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object FlinkTableApiExample2 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 开启事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))
    // 状态后端-HashMapStateBackend
    env.setStateBackend(new HashMapStateBackend)
    //等价于MemoryStateBackend
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.15-learning\\checkpoint")
    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    // 随机生产100条随机数据
    tableEnv.executeSql(
      s"""
         |CREATE TABLE source_table (
         |    id INT,
         |    score INT,
         |    address STRING,
         |    event_time TIMESTAMP_LTZ(3),
         |    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
         | ) WITH ('connector' = 'datagen',
         |         'rows-per-second'='5',
         |         'fields.id.kind'='sequence',
         |         'fields.id.start'='1',
         |         'fields.id.end'='100',
         |         'fields.score.min'='1',
         |         'fields.score.max'='100',
         |         'fields.address.length'='10')
         |""".stripMargin)

    val generatedTable: Table = tableEnv.from("source_table")

    import org.apache.flink.table.api._
    // 将table转成dataStream
/*    val dataStream: DataStream[Row] = tableEnv.toChangelogStream(
      generatedTable,
      Schema.newBuilder()
        .column("id", "INT")
        .column("score", "INT")
        .column("address", "STRING")
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build())
    dataStream.print()*/

    // 查询source_table表,各种sql操作
    val queryTable = tableEnv.sqlQuery("select * from source_table")
    val query2Table = tableEnv.sqlQuery("select id, address, SUM(score) as sumScore from source_table GROUP BY id, address")

    // table转成datastream
    val resultStream:DataStream[Row] = tableEnv.toChangelogStream(query2Table)
    resultStream.print()

    env.execute("FlinkTableApiExample2")

  }

}
