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
    bsTableEnv.executeSql(sql_analysis).print()




  }

}
