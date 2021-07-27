package com.bigdata.task.learn.analysis

import java.time.Duration

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, ExecutionCheckpointingOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

/**
 * create by young
 * date:20/12/6
 * desc:
 */
object Demo02Mysql {

  def main(args: Array[String]): Unit = {

    var logger: org.slf4j.Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    // 失败重启,固定间隔,每隔3秒重启1次,总尝试重启10次
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 3))
    // 本地测试线程 1
    env.setParallelism(1)

    // 事件处理的时间，由系统时间决定
     env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // checkpoint 设置
    val tableConfig = tEnv.getConfig.getConfiguration
    // 开启checkpoint
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    // checkpoint的超时时间周期，1 分钟做一次checkpoint, 每次checkpoint 完成后 sink 才会执行
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(60))
    // checkpoint的超时时间, 检查点一分钟内没有完成将被丢弃
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(60))
    // checkpoint 最小间隔，两个检查点之间至少间隔 30 秒
    tableConfig.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(30))
    // 同一时间只允许进行一个检查点
    tableConfig.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, Integer.valueOf(1))
    // 手动cancel时是否保留checkpoint
    tableConfig.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    /**
     * mysql 源表
     */
    val mysqlSourceSql =
      """
        |create table mysqlSourceTable (
        |  id int,
        |  name string,
        |  gender string,
        |  age int
        |) with (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://localhost:3306/analysis?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
        | 'username' = 'root',
        | 'password' = 'abc1314520',
        | 'table-name' = 'student',
        | 'driver' = 'com.mysql.cj.jdbc.Driver',
        | 'scan.fetch-size' = '200'
        |)
    """.stripMargin

    /**
     * mysql sink
     */
    val printSinkSql =
      """
        |create table printSinkTable (
        |  id int,
        |  name string,
        |  gender string,
        |  age int
        |) with (
        | 'connector' = 'print'
        |)
      """.stripMargin

    val writeMysqlTable =
      """
        |create table writeMysqlTable (
        |id int,
        |name string,
        |gender string,
        |age int
        |) with (
        | 'connector' = 'jdbc',
        | 'url' = 'jdbc:mysql://localhost:3306/analysis?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
        | 'username' = 'root',
        | 'password' = 'abc1314520',
        | 'table-name' = 'student',
        | 'driver' = 'com.mysql.cj.jdbc.Driver',
        | 'sink.buffer-flush.interval' = '3s',
        | 'sink.buffer-flush.max-rows' = '1',
        | 'sink.max-retries' = '5'
        |)
        """.stripMargin

    var insertSql = "insert into printSinkTable select * from mysqlSourceTable "
    insertSql = "insert into writeMysqlTable select * from mysqlSourceTable"

    tEnv.executeSql(mysqlSourceSql)
    tEnv.executeSql(writeMysqlTable)
    //    tEnv.executeSql(printSinkSql)
    tEnv.executeSql(insertSql)
    //    tEnv.executeSql("select * from mysqlSourceTable").print()
    tEnv.executeSql("select * from writeMysqlTable").print()

  }

}