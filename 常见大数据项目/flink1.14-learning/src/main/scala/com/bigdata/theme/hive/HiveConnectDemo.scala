package com.bigdata.theme.hive

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, Table, TableEnvironment, TableResult}
import org.apache.flink.table.catalog.hive.HiveCatalog

object HiveConnectDemo {

  def main(args: Array[String]): Unit = {

//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv = StreamTableEnvironment.create(env)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode().build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //	必须添加checkpoint
    env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    System.setProperty("HADOOP_USER_NAME","stats")

    val name = "myhive"
    val defaultDatabase = "default"
    val version = "1.1.0"

    // hive-site配置文件所在目录
    val hiveConfDir = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.14-learning\\src\\main\\resources"
    // val hiveConfDir = "/home/software/hive-2.1.1/conf"

    // 注册hive的Catalog
    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myhive", hive)

    // 使用注册的catalog
    tableEnv.useCatalog("myhive")
    tableEnv.executeSql("drop table if exists hw_clzp2")
    //创建kafka连接
    tableEnv.executeSql("""
                          |CREATE TABLE hw_clzp2 (
                          |   alarmId string,
                          |   alarmLevel string,
                          |  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
                          |  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                          |) WITH (
                          |  'connector' = 'kafka',
                          |  'topic' = 'mzpns',
                          |  'properties.bootstrap.servers' = 'localhost:9092',
                          |  'properties.group.id' = 'testGroup',
                          |  'scan.startup.mode' = 'latest-offset',
                          |  'format' = 'json',
                          |  'json.fail-on-missing-field' = 'false',
                          |  'json.ignore-parse-errors' = 'true'
                          |)""".stripMargin)

    //切换到hive方言创建hive表,auto-compaction和compaction.file-size设置hive小文件合并
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tableEnv.executeSql("show databases").print()
    tableEnv.executeSql("drop table if exists zhsq_clzp2")
    tableEnv.executeSql("""CREATE TABLE IF NOT EXISTS zhsq_clzp2 (
                          |   alarmId STRING,
                          |   alarmLevel STRING
                          |)
                          |PARTITIONED BY (dt STRING, hr STRING)
                          |STORED AS parquet TBLPROPERTIES (
                          |'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',
                          |'sink.partition-commit.trigger'='partition-time',
                          |'sink.partition-commit.delay'='0s',
                          |'sink.partition-commit.policy.kind'='metastore,success-file',
                          |'auto-compaction'='true',
                          |'compaction.file-size'='128MB'
                          |)""".stripMargin)

    // 切换到default方言
    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)

    // 向Hive表中写入数据
    tableEnv.executeSql("insert into zhsq_clzp2 select alarmId,alarmLevel,DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH') from hw_clzp2")


  }

}
