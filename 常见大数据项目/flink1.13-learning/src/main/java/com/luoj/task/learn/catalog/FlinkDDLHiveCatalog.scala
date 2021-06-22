package com.luoj.task.learn.catalog

import java.time.ZoneOffset._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings.newInstance
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment.create
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.module.hive.HiveModule
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.Text

object FlinkDDLHiveCatalog {

  private val catalog_name = "myhive"
  private val hive_version = "2.3.4"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = create(env, settings)
    tEnv.getConfig.setLocalTimeZone(ofHours(8))

    // 设置 early fired
    tEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true)
    tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "5000 ms")

    // 设置 job name
    tEnv.getConfig.getConfiguration.setString("pipeline.name",this.getClass.getSimpleName.replace("$",""))

    val catalog = new HiveCatalog(
      catalog_name,                                // catalog name
      "mydatabase",                // default database
      "/home/jason/bigdata/hive/hive-2.3.4",  // Hive config (hive-site.xml) directory
      hive_version                   // Hive version
    )

    // 注册 catalog
    tEnv.registerCatalog("myhive", catalog)
    // 选择一个 catalog
    tEnv.useCatalog("myhive")
    // 选择 database
    tEnv.useDatabase("mydatabase")
    // 加载 hive 的内置函数
    tEnv.loadModule(catalog_name,new HiveModule(hive_version))

    // kafka_source_jason 和 print_table 提前已经创建好可以直接使用
    tEnv.executeSql(
      """
        |insert into print_table
        |select
        |lower(funcName),
        |MIN(`timestamp`) as min_timestamp,
        |FIRST_VALUE(`timestamp`) as first_timestamp,
        |MAX(`timestamp`) as max_timestamp
        |from kafka_source_jason
        |group by funcName
        |""".stripMargin)
  }

}
