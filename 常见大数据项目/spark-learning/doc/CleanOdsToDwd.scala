package com.bidata.example.demos.etl

import java.io.File
import java.util.Properties

import com.sm.conf.ConfigurationManager
import com.sm.constants.Constants
import com.sm.utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
 * 二.
 * 数据清洗类
 * 将ods层数据进行清洗,导入dwd 层,以及配置汇总表的生成
 *
 * create by LiuJinHe 2019/10/23
 */
object CleanOdsToDwd {
  private val warehouseLocation = "hdfs://cdh-slave01:9870/user/hive/warehouse"
  //  private val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  private val logger = LoggerFactory.getLogger("CleanOdsToDwd")
  private var prop: Properties = new Properties
  private var yesterday: String = _
  var startDate = ""
  var endDate = ""

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.spark_project.jetty").setLevel(Level.WARN)

    // 获取当天和前一天日期

    yesterday = DateUtils.getYesterdayDate

    if (args.length == 2) {
      yesterday = args(0)
    }
    // startDate = "2019-01-01"
    // endDate = "2019-10-22"

    // 初始化spark
    val spark = initSparkSession

    logger.info("===============> 开始生成 conf_game_package_channel 配置汇总表 <===============")
    makePackageConf(spark)

    // 读取ODS 层数据,解析导入 DWD 层事件表
    val start = System.currentTimeMillis()
    logger.info(s"===================> 开始加载Hive ODS层数据进行清洗 <===================")

    prop = loadProp(Constants.HIVE_DW)

    // 清洗数据
    val database = prop.getProperty("hive.database")
    var odsTables = ""

    odsTables = prop.getProperty("ods.sdk.user.login.table")
    cleanData(spark, database, odsTables)

    odsTables = prop.getProperty("ods.sdk.active.table")
    cleanData(spark, database, odsTables)

    val end = System.currentTimeMillis()
    println("耗时：" + (end - start))

    logger.info("===================> 清洗结果数据写入Hive dwd层完成 <===================")
    spark.stop()
  }

  /**
   * 数据清洗
   */
  def cleanData(spark: SparkSession, db: String, table: String): Unit = {
    /**
     * 清洗规则：
     * ① time过滤 0000-00-00 00:00:00
     * ② 过滤 game_id <= 0, game_id =339, package_id <=9 的数据
     * ③ 所有出现字符串都统⼀转成小写格式
     * ④ 通过gameId与conf_base_game_表匹配获得cp_game_id,过滤未匹配到,或cp_game_id<=0 的数据
     * ⑤ login_account后面带\t 等特殊切割符合的数据进行过滤（会判断为非时间类型）
     * 其中,屏幕宽高是两个单独的字段,合并为一个字段
     */

    // 加载 gameId 配置表
    val gameIdTable = "conf_base_game"
    spark.table(s"$db.$gameIdTable").createOrReplaceTempView("confTable")

    // 加载日志数据
    val dataFrame = spark.sql(s"select * from $db.$table where `date`='$yesterday'")
    // var sqlStr = s"select * from $db.$table where `date`>= '$startDate' and `date` < '$endDate'"
    val dataFrame = spark.sql(sqlStr)
      .persist(StorageLevel.MEMORY_ONLY)
    dataFrame.createOrReplaceTempView("tmpTable")

    var destTable = ""

    // 清洗数据
    if (table.equals("ods_sdk_user_login")) {
      destTable = prop.getProperty("dwd.sdk.user.login.table")
      sqlStr =
        s"""
           |select
           | a.id,
           | a.game_id,
           | a.package_id,
           | conf.cp_game_id,
           | lower(rtrim("\t",a.login_account)) as login_account,
           | lower(a.core_account) as core_account,
           | a.time,
           | a.time_server,
           | lower(a.device_id) as  device_id,
           | lower(a.md5_device_id) as  md5_device_id,
           | lower(a.device_code) as  device_code,
           | lower(a.device_key) as  device_key,
           | lower(a.android_id) as  android_id,
           | lower(a.useragent) as  useragent,
           | lower(a.device_type) as  device_type,
           | a.os,
           | a.os_version,
           | a.sdk_version,
           | a.game_version,
           | lower(a.network_type) as  network_type,
           | a.mobile_type,
           | concat(a.screen_width,(case a.screen_height when '' then '' else 'x' end),a.screen_height) as width_height,
           | a.ip,
           | a.lbs,
           | lower(a.refer) as  refer,
           | lower(a.refer_param) as refer_param,
           | a.channel_alter,
           | a.date
           |from confTable as conf join tmpTable as a
           | where a.game_id = conf.game_id
           | and a.game_id > 0
           | and a.game_id != 339
           | and a.package_id > 9
           | and conf.cp_game_id > 0
           | and a.time is not null
            """.stripMargin
    } else if (table.equals("ods_sdk_active_log")) {
      destTable = prop.getProperty("dwd.sdk.active.table")
      sqlStr =
        s"""
           |select
           | a.id,
           | a.game_id,
           | a.package_id,
           | conf.cp_game_id,
           | a.time,
           | a.time_server,
           | lower(a.device_id) as device_id,
           | lower(a.md5_device_id) as md5_device_id,
           | lower(a.device_code) as device_code,
           | lower(a.android_id) as android_id,
           | lower(serial_number) as serial_number,
           | lower(a.device_key) as device_key,
           | lower(a.useragent) as useragent,
           | lower(a.device_type) as device_type,
           | a.os,
           | a.os_version,
           | a.sdk_version,
           | a.game_version,
           | lower(a.network_type) as network_type,
           | a.mobile_type,
           | concat(a.screen_width,(case a.screen_height when '' then '' else 'x' end),a.screen_height) as width_height,
           | a.ip,
           | a.lbs,
           | lower(a.refer) as refer,
           | lower(a.refer_param) as refer_param,
           | a.channel_id,
           | a.sv_key,
           | a.click_id,
           | a.click_time,
           | date
           |from confTable as conf join tmpTable as a
           | where a.game_id = conf.game_id
           | and a.game_id > 0
           | and a.game_id != 339
           | and a.package_id > 9
           | and conf.cp_game_id > 0
           | and a.time is not null
        """.stripMargin
    }

    import spark.sql

    // 数据清洗后写入Hive dwd层表当日分区中
    // 重分区,会将提交时候设置的num-executors计算结果合并为一个,输出每个date分区一个文件
    sql(sqlStr).repartition(1).createOrReplaceTempView("resultTable")
    sqlStr =
      s"""
         |insert overwrite table $db.$dayTable
         | partition(`date`)
         |select * from resultTable
       """.stripMargin
    sql(sqlStr)

    // 或者,重分区比较耗时
    // sql(sqlStr).coalesce(1).write.mode(SaveMode.Append).insertInto(s"$db.$destTable")


    // 或者
    // sql(sqlStr).coalesce(1).createOrReplaceTempView("resultTable")
    // sqlStr = "select * from resultTable"
    // sql(sqlStr).write.mode(SaveMode.Append).insertInto(s"$db.$destTable")

    // 或者
    // sql(sqlStr).coalesce(1).createOrReplaceTempView("resultTable")
    // spark.table("resultTable").write.mode(SaveMode.Append).insertInto(s"$db.$destTable")

    dataFrame.unpersist(true)
  }

  /**
   * 根据配置表生成Hive汇总表 conf_game_package_channel
   */
  def makePackageConf(spark: SparkSession): Unit = {
    prop = loadProp(Constants.HIVE_CONF_TABLE)
    val db = prop.getProperty("hive.database")
    val packageIdTable = prop.getProperty("conf.base.package.table")
    val gameIdTable = prop.getProperty("conf.base.game.table")
    val channelIdTable = prop.getProperty("conf.base.channel.table")
    val summaryConfTable = prop.getProperty("conf.game.package.channel.table")

    val packageDF = spark.table(s"$db.$packageIdTable")
    val gameDF = spark.table(s"$db.$gameIdTable")
    val channelDF = spark.table(s"$db.$channelIdTable")

    val summaryConfDF = packageDF.join(gameDF, "game_id").join(channelDF, "CHANNEL_ID")
    summaryConfDF.createOrReplaceTempView("tmpConfTable")

    val sqlStr =
      s"""
         |insert overwrite table $db.$summaryConfTable
         |select
         | package_id,game_id,cp_game_id,sm_game_name,
         | popularize_v1_id,popularize_v2_id,channel_main_id,channel_main_name,channel_id,channel_name,
         | chan_id,channel_code,channel_label,platform_id,promotion_way_id
         |from tmpConfTable
      """.stripMargin

    spark.sql(sqlStr)
    logger.info(s"========== 生成 $summaryConfTable 汇总表成功! ==========")
  }

  // 加载配置
  def loadProp(properties: String): Properties = {
    val props = new Properties()
    val in = ConfigurationManager.getClass.getClassLoader.getResourceAsStream(properties)
    props.load(in)
    props
  }

  def initSparkSession: SparkSession = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master(Constants.SPARK_YARN_CLIENT_MODE)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("hive.exec.max.dynamic.partitions", 2000)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer", "1024m")
    .config("spark.kryoserializer.buffer.max", "2046m")
    .config("spark.io.compression.codec", "snappy")
    .config("spark.sql.codegen", "true")
    .config("spark.sql.unsafe.enabled", "true")
    .config("spark.shuffle.manager", "tungsten-sort")
    .enableHiveSupport()
    .getOrCreate()
}
