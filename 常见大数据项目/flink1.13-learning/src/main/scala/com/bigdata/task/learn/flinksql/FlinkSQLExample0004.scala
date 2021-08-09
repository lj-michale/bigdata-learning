package com.bigdata.task.learn.flinksql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @author lj.michale
 * @description  随机数据生成器- DataGen connector
 * @date 2021-08-02
 */
object FlinkSQLExample0004 {

  def main(args: Array[String]): Unit = {

    val bbSettings = EnvironmentSettings.newInstance.useBlinkPlanner.build
    val bsTableEnv = TableEnvironment.create(bbSettings)

    val sourceDDL =
      """
        |CREATE TABLE datagen (
        | f_random INT,
        | f_random_str STRING,
        | ts AS localtimestamp,
        | WATERMARK FOR ts AS ts
        |) WITH (
        | 'connector' = 'datagen',
        | 'rows-per-second'='10',
        | 'fields.f_random.min'='1',
        | 'fields.f_random.max'='5',
        | 'fields.f_random_str.length'='10'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(sourceDDL)

    val sourceDDL2 =
      """
        |CREATE TABLE source_table (
        |    id INT,
        |	score INT,
        |	address STRING,
        |	ts AS localtimestamp,
        |	WATERMARK FOR ts AS ts
        |) WITH (
        |   'connector' = 'datagen',
        |   'rows-per-second'='5',
        |   'fields.id.kind'='sequence',
        |   'fields.id.start'='1',
        |   'fields.id.end'='100',
        |   'fields.score.min'='1',
        |   'fields.score.max'='100',
        |   'fields.address.length'='10'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(sourceDDL2)

    val queryDDL = "select * from datagen"
    val queryDDL2 = "select * from source_table"

//    bsTableEnv.executeSql(queryDDL).print()
    //bsTableEnv.executeSql(queryDDL2).print()

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * 参数	是否必选	默认值	数据类型	描述
     * connector 必须	(none)	String	指定要使用的连接器，这里是 'datagen'。
     * rows-per-second 可选	10000	Long	每秒生成的行数，用以控制数据发出速率。
     * fields.#.kind 可选	random	String	指定 '#' 字段的生成器。可以是 'sequence' 或 'random'。
     * fields.#.min 可选	(Minimum value of type)	(Type of field)	随机生成器的最小值，适用于数字类型。
     * fields.#.max 可选	(Maximum value of type)	(Type of field)	随机生成器的最大值，适用于数字类型。
     * fields.#.length 可选	100	Integer	随机生成器生成字符的长度，适用于 char、varchar、string。
     * fields.#.start 可选	(none)	(Type of field)	序列生成器的起始值。
     * fields.#.end 可选	(none)	(Type of field)	序列生成器的结束值。
     */
    val datagen_ds_one =
      """
        |CREATE TABLE datagen_test_source (
        | f_sequence INT,
        | f_random INT,
        | f_random_str STRING,
        | ts AS localtimestamp,
        | WATERMARK FOR ts AS ts
        |) WITH (
        | 'connector' = 'datagen',
        | -- optional options --
        | 'rows-per-second'='5',  -- 每秒生成的数据条数
        | 'fields.f_sequence.kind'='sequence',
        | 'fields.f_sequence.start'='1',
        | 'fields.f_sequence.end'='1000',
        | 'fields.f_random.min'='1',
        | 'fields.f_random.max'='1000',
        | 'fields.f_random_str.length'='10'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(datagen_ds_one)
//    bsTableEnv.executeSql("select * from datagen_test_source").print()

    /********************************   Look up维表的使用   *********************************************/
    // 构造订单数据, 模拟生成用户的数据，这里只生成的用户的id，范围在1-100之间
    val datagen_order_source =
      """
        |CREATE TABLE datagen_order_source (
        |  userid INT,
        |  proctime as PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |   -- 可选项 --
        |  'rows-per-second'='10',
        |  'fields.userid.kind'='random',
        |  'fields.userid.min'='1',
        |  'fields.userid.max'='100'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(datagen_order_source)
//    bsTableEnv.executeSql("select * from datagen_order_source").print()

    // 创建一个mysql维表信息
    val datagen_dim_source =
      """
        |CREATE TABLE datagen_dim_source (
        |  id int,
        |  name STRING,
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://localhost:3306/bigdata?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true',
        |   'table-name' = 'userinfo',
        |   'username' = 'root',
        |   'password' = 'abc1314520'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(datagen_dim_source)

    val sql_analysis =
      """
        |SELECT * FROM datagen_order_source
        |LEFT JOIN datagen_dim_source FOR SYSTEM_TIME AS OF datagen_order_source.proctime
        |ON datagen_order_source.userid = datagen_dim_source.id
        |""".stripMargin
//    bsTableEnv.executeSql(sql_analysis).print()

    /********************************   FlinkSQL的使用   *********************************************/
    // 维表存储在MySQL中，如下创建维表数据源：
    val mysql_region_dim_source =
      """
        |CREATE TABLE dim_province (
        |    province_id INT,  -- 省份id
        |    province_name  VARCHAR, -- 省份名称
        |    region_name VARCHAR -- 区域名称
        |) WITH (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://localhost:3306/jiguang?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true',
        |    'table-name' = 'dim_province',
        |    'driver' = 'com.mysql.cj.jdbc.Driver',
        |    'username' = 'root',
        |    'password' = 'abc1314520',
        |    'lookup.cache.max-rows' = '5000',
        |    'lookup.cache.ttl' = '10min'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(mysql_region_dim_source)

    // 创建kafka数据源表
    val kafka_order_source =
      """
        |CREATE TABLE user_behavior (
        |    user_id BIGINT, -- 用户id
        |    item_id BIGINT, -- 商品id
        |    cat_id BIGINT,  -- 品类id
        |    action STRING,  -- 用户行为
        |    province INT,   -- 用户所在的省份
        |    ts BIGINT,  -- 用户行为发生的时间戳
        |    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列
        |    eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss')), -- 事件时间
        |    WATERMARK FOR eventTime as eventTime - INTERVAL '5' SECOND  -- 在eventTime上定义watermark
        |) WITH (
        | 'connector' = 'kafka-0.11',
        | 'topic' = 'mzpns',
        | 'properties.bootstrap.servers' = 'localhost:9092',
        | 'properties.group.id' = 'testGroup',
        | 'format' = 'json',
        | 'scan.startup.mode' = 'latest-offset'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(kafka_order_source)
    bsTableEnv.executeSql("select * from user_behavior").print()

    // 创建MySQL的结果表，表示区域销量
    val result_analysis_sink =
      """
        |CREATE TABLE region_sales_sink (
        |    region_name STRING,  -- 区域名称
        |    buy_cnt BIGINT  -- 销量
        |) WITH (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://localhost:3306/jiguang?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true',
        |    'table-name' = 'top_region', -- MySQL中的待插入数据的表
        |    'driver' = 'com.mysql.cj.jdbc.Driver',
        |    'username' = 'root',
        |    'password' = 'abc1314520',
        |    'write.flush.interval' = '1s'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(result_analysis_sink)

      // 用户行为数据与省份维表数据join
//      val join_analysis =
//        """
//          |CREATE VIEW user_behavior_detail AS
//          |SELECT
//          |  u.user_id,
//          |  u.item_id,
//          |  u.cat_id,
//          |  u.action,
//          |  p.province_name,
//          |  p.region_name
//          |FROM user_behavior AS u LEFT JOIN dim_province FOR SYSTEM_TIME AS OF u.proctime AS p
//          |ON u.province = p.province_id
//          |""".stripMargin
//    bsTableEnv.executeSql(join_analysis)

    // 计算区域的销量，并将计算结果写入MySQL
//    val data_analysis =
//      """
//        |INSERT INTO region_sales_sink
//        |SELECT
//        |  region_name,
//        |  COUNT(*) buy_cnt
//        |FROM user_behavior_detail
//        |WHERE action = 'buy'
//        |GROUP BY region_name
//        |""".stripMargin
//    bsTableEnv.executeSql(data_analysis)

//    val data_analysis_print =
//      """
//        |INSERT INTO region_sales_sink
//        |SELECT
//        |  region_name,
//        |  COUNT(*) buy_cnt
//        |FROM (
//        |	SELECT
//        |	  u.user_id,
//        |	  u.item_id,
//        |	  u.cat_id,
//        |	  u.action,
//        |	  p.province_name,
//        |	  p.region_name
//        |	FROM user_behavior AS u LEFT JOIN dim_province FOR SYSTEM_TIME AS OF u.proctime AS p
//        |	ON u.province = p.province_id)
//        |WHERE action = 'buy'
//        |GROUP BY region_name
//        |""".stripMargin
//    bsTableEnv.executeSql(data_analysis_print).print()



  }

}
