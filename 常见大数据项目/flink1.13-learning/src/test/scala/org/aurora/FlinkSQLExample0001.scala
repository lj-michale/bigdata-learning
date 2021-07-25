package org.aurora

import java.time.ZoneOffset.ofHours
import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.slf4j.{Logger, LoggerFactory}
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.api.scala._


/**
 * @author lj.michale
 * @description
 * @date 2021-07-24
 */
object FlinkSQLExample0001 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")
    env.setStateBackend(new EmbeddedRocksDBStateBackend)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    tEnv.getConfig.setLocalTimeZone(ofHours(8))
//    tEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true)
//    tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "5000 ms")
//    tEnv.getConfig.getConfiguration.setString("pipeline.name",this.getClass.getSimpleName.replace("$",""))

//    /**
//     * Hive Catalog
//     * */
//    val name = "hive"
//    val defaultDatabase = "stream_tmp"
//    val hiveConfDir = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\src\\main\\resources"
//    val version = "1.1.0"
//    val hive:HiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
//    tEnv.registerCatalog("hive", hive)
//    tEnv.useCatalog("hive")

    val kafkaProperties: Properties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProperties.setProperty("group.id", "event")
    kafkaProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT)
    val KAFKA_ORDER_DDL =
     """
       |CREATE TABLE kafka_source(
       |    funcName STRING,
       |    data ROW<snapshots ARRAY<ROW<content_type STRING,url STRING>>,audio ARRAY<ROW<content_type STRING,url STRING>>>,
       |    resultMap ROW<`result` MAP<STRING,STRING>,isSuccess BOOLEAN>,
       |    meta  MAP<STRING,STRING>,
       |    `type` INT,
       |    `timestamp` BIGINT,
       |    arr ARRAY<ROW<address STRING,city STRING>>,
       |    map MAP<STRING,INT>,
       |    doublemap MAP<STRING,MAP<STRING,INT>>,
       |    proctime as PROCTIME()
       |) WITH (
       |    'connector' = 'kafka', -- 使用 kafka connector
       |    'topic' = 'mzpns',  -- kafka topic
       |    'properties.bootstrap.servers' = 'localhost:9092',  -- broker连接信息
       |    'properties.group.id' = 'jason_flink_test', -- 消费kafka的group_id
       |    'scan.startup.mode' = 'latest-offset',  -- 读取数据的位置
       |    'format' = 'json',  -- 数据源格式为 json
       |    'json.fail-on-missing-field' = 'true', -- 字段丢失任务不失败
       |    'json.ignore-parse-errors' = 'false'  -- 解析失败跳过
       |)
       |""".stripMargin
    tEnv.executeSql(KAFKA_ORDER_DDL)

    val AnalysiSQL =
      """
        |select
        |funcName,
        |doublemap['inner_map']['key'],
        |count(data.snapshots[1].url),
        |`type`,
        |TUMBLE_START(proctime, INTERVAL '30' second) as t_start
        |from kafka_source
        |group by TUMBLE(proctime, INTERVAL '30' second),funcName,`type`,doublemap['inner_map']['key']
        |""".stripMargin
    val AnalysisSQLResult = tEnv.sqlQuery(AnalysiSQL)

    env.execute("FlinkSQLExample0001")

    logger.info("FlinkSQL计算结束...")

  }



}
