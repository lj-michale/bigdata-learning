package com.bigdata.task.table

import java.text.SimpleDateFormat
import java.util._
import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.CheckpointingOptions
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

trait BaseFlinkStreaming extends BaseFlink {

  def init(args: Array[String]): Array[String] = args


  // env
  var tEnv:StreamTableEnvironment = _
  val parallelism = 1

  // checkpoint
  val restartAttempts = 3
  val failureInterval = 1800
  val delayInterval = 120
  val checkpointInterval = 600000
  val checkpointingMode = CheckpointingMode.AT_LEAST_ONCE

  // sql
  val stateRetentionMinTime = 1
  val stateRetentionMxnTime = 2

  // kafka
  val bootstrapServers = ""
  // 默认 昨日00:00
  val kafkaStartTime = getKafkaStartTime

  /**
   * 初始化flink运行时环境
   */
  override def createContext(conf: Any): Unit = {
    super.createContext(conf)

    // val jobParallelism = PropUtils.getInt("flink.default.parallelism", parallelism)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    env.enableCheckpointing(checkpointInterval, checkpointingMode)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(checkpointInterval)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    env.setRestartStrategy(
      RestartStrategies.failureRateRestart(restartAttempts,
        Time.of(failureInterval,TimeUnit.SECONDS),
        Time.of(delayInterval,TimeUnit.SECONDS))
    )

    tEnv = StreamTableEnvironment.create(env,settings)
    tEnv.getConfig.setIdleStateRetentionTime(Time.days(stateRetentionMinTime), Time.days(stateRetentionMxnTime))

    val config = tEnv.getConfig.getConfiguration
    config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true)

  }

  /**
   * 获取昨日 00:00 时间点
   * @return
   */
  def getKafkaStartTime():String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val c = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.DATE, -1)
    format.format(c.getTime) + " 00:00:00"
  }

  def main(args: Array[String]): Unit = {
    this.init(args)
  }


}
