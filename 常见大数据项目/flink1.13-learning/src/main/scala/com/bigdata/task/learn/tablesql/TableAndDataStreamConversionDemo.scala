package com.bigdata.task.learn.tablesql

import java.time.ZoneId

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._


/**
 * @description 将表转换成 DataStream
 *              Table 可以被转换成 DataStream。 通过这种方式，定制的 DataStream 程序就可以在 Table API 或者 SQL 的查询结果上运行了。
 *              将 Table 转换为 DataStream 时，你需要指定生成的 DataStream 的数据类型，即，Table 的每行数据要转换成的数据类型。 通常最方便的选择是转换成 Row 。
 *              以下列表概述了不同选项的功能：
 *              Row: 字段按位置映射，字段数量任意，支持 null 值，无类型安全（type-safe）检查。
 *              POJO: 字段按名称映射（POJO 必须按Table 中字段名称命名），字段数量任意，支持 null 值，无类型安全检查。
 *              Case Class: 字段按位置映射，不支持 null 值，有类型安全检查。
 *              Tuple: 字段按位置映射，字段数量少于 22（Scala）或者 25（Java），不支持 null 值，无类型安全检查。
 *              Atomic Type: Table 必须有一个字段，不支持 null 值，有类型安全检查。
 *
 *              流式查询（streaming query）的结果表会动态更新，即，当新纪录到达查询的输入流时，查询结果会改变。因此，像这样将动态查询结果转换成 DataStream 需要对表的更新方式进行编码。
 *              将 Table 转换为 DataStream 有两种模式：
 *              Append Mode: 仅当动态 Table 仅通过INSERT更改进行修改时，才可以使用此模式，即，它仅是追加操作，并且之前输出的结果永远不会更新。
 *              Retract Mode: 任何情形都可以使用此模式。它使用 boolean 值对 INSERT 和 DELETE 操作的数据进行标记。
 *
 *              https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/data_stream_api/
 * @author lj.michale
 * @date 2021-07-16
 */
object TableAndDataStreamConversionDemo {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    // create environments of both APIs

    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    // Table with two fields (String name, Integer age)
    val table: Table = tableEnv.fromValues()

    env.execute(s"${this.getClass.getName}")

  }

}
