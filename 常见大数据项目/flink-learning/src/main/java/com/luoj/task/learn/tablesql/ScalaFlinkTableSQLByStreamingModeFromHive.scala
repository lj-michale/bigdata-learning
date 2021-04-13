//package com.luoj.task.learn.tablesql
//
//import com.luoj.common.Logging
//import javax.print.DocFlavor.STRING
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.table.api.{EnvironmentSettings, GroupedTable, SqlDialect, Table, TableResult}
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
//import org.apache.flink.table.catalog.hive.HiveCatalog
//import org.apache.flink.configuration.ConfigConstants
//import org.apache.flink.streaming.api.scala.DataStream
//import org.apache.flink.types.Row
//import org.slf4j.LoggerFactory
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.datastream.DataStreamSource
//import org.apache.flink.table.api._
//import org.apache.flink.table.api.bridge.scala._
//
///**
// * @descr 0000000000000
// * @date 2021/4/12 17:26
// */
//object ScalaFlinkTableSQLByStreamingModeFromHive {
//
//  val logger = LoggerFactory.getLogger(this.getClass)
//
//  def main(args: Array[String]): Unit = {
//
//    // 本地Idea调试
//    val conf: Configuration = new Configuration()
//    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
//    val streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
//    // 连接Flink调试
////    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment()
//    streamEnv.enableCheckpointing(10000)
//    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    streamEnv.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
//    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    streamEnv.setParallelism(1)
//    val blinkTableEnvSettings = EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      .inStreamingMode()
//      .build()
//    val tableEnv = StreamTableEnvironment.create(streamEnv, blinkTableEnvSettings)
//
//    val name = "hive"
//    val defaultDatabase = "stream_tmp"
//    val hiveConfDir = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink-learning\\src\\main\\resources"
//    val version = "1.1.0"
//    logger.info("name:{}, defaultDatabase:{}, hiveConfDir:{}, version:{}", name, defaultDatabase, hiveConfDir, version)
//
//    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
//    tableEnv.registerCatalog("hive", hive)
//    tableEnv.useCatalog("hive")
//
//    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
//
//    val dataAnalysisSQL:String =
//      """
//        |SELECT app_key, platform, channel, sdk_ver, num FROM report.android_awake_day_report
//        |""".stripMargin
//
//    // ///////////////////////////  tableEnv.executeSql  ///////////////////////////////
//    // val tableResult:TableResult = tableEnv.executeSql(dataAnalysisSQL)
//    // tableResult.print()
//    // logger.info("tableResult.getTableSchema.getFieldNames:{}", tableResult.getTableSchema.getFieldNames)
//
//    // ///////////////////////////  tableEnv.sqlQuery  ///////////////////////////////
//    val tableRes:Table = tableEnv.sqlQuery(dataAnalysisSQL)
//    val tableReByGroup:GroupedTable = tableRes.groupBy("platform")
//    val explaination: String = tableEnv.explain(tableRes)
//    println(explaination)
//    // println(tableResult.getJobClient.get().getJobStatus)
//
//    // ////////////////////////// Table、DataSet、DataStream相互转换 //////////////////
//    // convert the Table into an append DataStream of Row
//    // conversion to DataSet
////    val result:DataSet[Row] = tableRes
////      .groupBy($"a")
////      .select($"a", $"b".count as "cnt")
////      .toDataSet[Row] // conversion to DataSet
//    println("tableRes.getSchema:" + tableRes.getSchema)
////    val dataSet = tableRes.groupBy("platform").select($"platform", $"num".count() as "numCount").toDataSet[Row]
////    val dataSet:DataSet[Row] = tableRes.toDataSet[Row]
////    val resStream:org.apache.flink.streaming.api.datastream.DataStream[Row] = tableEnv.toAppendStream(tableRes, classOf[org.apache.flink.types.Row])
////    resStream.print()
//
//
//    // tableResult -> DataStream
//    // 将Table转换成DataStream
//
//    val table = tableEnv.fromValues(
//      row(1, "ABC"),
//      row(2L, "ABCDE")
//    )
//
//    val table2 = tableEnv.fromValues(
//      DataTypes.ROW(
//        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
//        DataTypes.FIELD("name", DataTypes.STRING())
//      ),
//      row(1, "ABC"),
//      row(2L, "ABCDE")
//    )
//
//    val dataSet:DataStreamSource[(Int, String, Int, String, Int)] = streamEnv.fromElements((1,"小明",15,"男",1500),(2,"小王",45,"男",4000),(3,"小李",25,"女",800),(4,"小慧",35,"女",500))
//    val dataSetGrade:DataStreamSource[(Int, String, Int)] = streamEnv.fromElements((1,"语文",100),(2,"数学",80),(1,"外语",50) )
//    val table3 = tableEnv.fromDataStream(dataSet)
//    val table4:Table = tableEnv.fromDataStream(dataSetGrade)
//
//    //tableEnv.registerDataSet("user1",dataSet,'id,'name,'age,'sex,'salary)
//
//    // DataStream
//    val orderA: DataStream[Order] = streamEnv.fromCollection(Seq(
//      Order(2L, "pen", 3),
//      Order(1L, "rubber", 3),
//      Order(4L, "beer", 1)
//    ))
//
//    // register DataStream as Table
//    tableEnv.registerDataStream("OrderA", orderA, 'user, 'product, 'amount)
//
//  }
//
//}
//
//case class tableRe(app_key: String, platform: String, channel: String, sdk_ver: String, num: Int)
//case class Order(user: Long, product: String, amount: Int)
