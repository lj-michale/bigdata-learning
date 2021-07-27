package com.bigdata.task.learn.catalog

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.slf4j.{Logger, LoggerFactory}


/**
 * @author lj.michale
 * @description DataAnalysisByHive001
 * @date 2021-05-20
 */
object DataAnalysisByHive001 {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // 流处理模式
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val tableEnv = StreamTableEnvironment.create(env, setting)

    // 批处理模式
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tableEnv = TableEnvironment.create(setting)

    val name = "myhive"
    val defaultDatabase = "default"
    val hiveConfDir = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.11-learning\\src\\main\\resources"
    val version = "1.1.0"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myhive", hive)
    tableEnv.useCatalog("myhive")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    // 查看数据库
    tableEnv.executeSql("show databases").print()
//    +-----------------------------------------------------------------------------------------------+
//    |                                                                                 database name |
//      +-----------------------------------------------------------------------------------------------+
//    | cloudera_manager_metastore_canary_test_db_hive_hivemetastore_b8479842d123a758eea13010c3465f5e |
//    |                                                                                       default |
//    |                                                                                           dsg |
//    |                                                                                        dsg_dw |
//    |                                                                                           edw |
//    |                                                                                          hawk |
//    |                                                                                   impala_kudu |
//    |                                                                                   kudu_report |
//    |                                                                                        public |
//    |                                                                                        report |
//    |                                                                                           tmp |
//    +-----------------------------------------------------------------------------------------------+
    tableEnv.executeSql("USE report")
    tableEnv.executeSql("SHOW TABLES").print()

//    +-----------------------------+
//    |                  table name |
//      +-----------------------------+
//    | event_user_group_day_report |
//    |                    houshuai |
//    |               houshuai_test |
//    |    retention_result_routine |
//    |     retention_result_single |
//    +-----------------------------+

    // 数据查询
    tableEnv.executeSql("SELECT * FROM report.event_user_group_day_report").print()




  }

}
