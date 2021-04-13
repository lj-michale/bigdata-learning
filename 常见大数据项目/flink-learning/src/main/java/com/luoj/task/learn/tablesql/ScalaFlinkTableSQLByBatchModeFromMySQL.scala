//package com.luoj.task.learn.tablesql
//
//import java.time.Duration
//
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.streaming.api.{CheckpointingMode}
//import org.apache.flink.streaming.api.environment.{CheckpointConfig, ExecutionCheckpointingOptions}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//
///**
// * @descr
// * @date 2021/4/12 10:49
// */
//object ScalaFlinkTableSQLByBatchModeFromMySQL {
//
//  def main(args: Array[String]): Unit = {
//
//    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv:StreamTableEnvironment = StreamTableEnvironment.create(env)
//
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 3))
//    env.setParallelism(1)
//    // checkpoint 设置
//    val tableConfig = tableEnv.getConfig.getConfiguration
//    // 开启checkpoint
//    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
//    // checkpoint的超时时间周期，1 分钟做一次checkpoint, 每次checkpoint 完成后 sink 才会执行
//    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(60))
//    // checkpoint的超时时间, 检查点一分钟内没有完成将被丢弃
//    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(60))
//    // checkpoint 最小间隔，两个检查点之间至少间隔 30 秒
//    tableConfig.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(30))
//    // 同一时间只允许进行一个检查点
//    tableConfig.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, Integer.valueOf(1))
//    // 手动cancel时是否保留checkpoint
//    tableConfig.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//
//    /**
//     * mysql 源表
//     */
//    val mysqlSourceSql =
//      """
//        |create table mysqlSourceTable (
//        |  id int,
//        |  name string,
//        |  gender string,
//        |  age int
//        |) with (
//        | 'connector' = 'jdbc',
//        | 'url' = 'jdbc:mysql://localhost:3306/jiguang?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
//        | 'username' = 'root',
//        | 'password' = 'abc1314520',
//        | 'table-name' = 'student',
//        | 'driver' = 'com.mysql.cj.jdbc.Driver',
//        | 'scan.fetch-size' = '200'
//        |)
//    """.stripMargin
//
//    /**
//     * mysql sink
//     */
//    val printSinkSql =
//      """
//        |create table printSinkTable (
//        |  id int,
//        |  name string,
//        |  gender string,
//        |  age int
//        |) with (
//        | 'connector' = 'print'
//        |)
//      """.stripMargin
//
//    val writeMysqlTable =
//      """
//        |create table writeMysqlTable (
//        |id int,
//        |name string,
//        |gender string,
//        |age int
//        |) with (
//        | 'connector' = 'jdbc',
//        | 'url' = 'jdbc:mysql://localhost:3306/jiguang?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC',
//        | 'username' = 'root',
//        | 'password' = 'abc1314520',
//        | 'table-name' = 'student',
//        | 'driver' = 'com.mysql.cj.jdbc.Driver',
//        | 'sink.buffer-flush.interval' = '3s',
//        | 'sink.buffer-flush.max-rows' = '1',
//        | 'sink.max-retries' = '5'
//        |)
//      """.stripMargin
//
//    var insertSql = "insert into printSinkTable select * from mysqlSourceTable "
//        insertSql = "insert into writeMysqlTable select * from mysqlSourceTable"
//
//    tableEnv.executeSql(mysqlSourceSql)
//    tableEnv.executeSql(writeMysqlTable)
//    tableEnv.executeSql(printSinkSql)
//    tableEnv.executeSql(insertSql)
//    tableEnv.executeSql("select * from mysqlSourceTable").print()
//    tableEnv.executeSql("select * from writeMysqlTable").print()
//
//  }
//
//}
