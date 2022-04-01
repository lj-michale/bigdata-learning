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

    /**
     * DataGen Connector用于在Table和SQL API中生成数据。底层实现就用到了上面的DataGenerator。
     * 连接器参数
     * 参数	是否必选	默认参数	数据类型	描述
     * connector	必须	(none)	String	指定要使用的连接器，这里是 ‘datagen’。
     * rows-per-second	可选	10000	Long	每秒生成的行数，用以控制数据发出速率。
     * fields.#.kind	可选	random	String	指定 ‘#’ 字段的生成器。可以是 ‘sequence’ 或 ‘random’。
     * fields.#.min	可选	(Minimum value of type)	(Type of field)	随机生成器的最小值，适用于数字类型。
     * fields.#.max	可选	(Maximum value of type)	(Type of field)	随机生成器的最大值，适用于数字类型。
     * fields.#.length	可选	100	Integer	随机生成器生成字符的长度，适用于 char、varchar、string。
     * fields.#.start	可选	(none)	(Type of field)	序列生成器的起始值。
     * fields.#.end	可选	(none)	(Type of field)	序列生成器的结束值。
     */
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
