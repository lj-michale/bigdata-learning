package com.bigdata.feature.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @descri
 * @author lj.michale
 * @date 2022-04-28
 */
object TableSqlTest {

  def main(args: Array[String]): Unit = {

    // 定义Table环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    // 表定义了primary key，则以upsert(更新插入)方式插入数据
    val table_str =
      """
        |create temporary table %s(
        |  id bigint,
        |  name string,
        |  age int,
        |  primary key (id) not enforced
        |) with (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://localhost:3306/zengame?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=GMT',
        |   'driver' = 'com.mysql.cj.jdbc.Driver',
        |   'table-name' = '%s',
        |   'username' = 'root',
        |   'password' = 'abc1314520'
        |)
        |""".stripMargin
    // 在catalog注册表
    tEnv.executeSql(table_str.format("user1", "user1"))
    tEnv.executeSql(table_str.format("user2", "user2"))

    // =====================读取源表数据=====================
    val user1 = tEnv.from("user1")   // 方式一
//    user1.execute().print()
    // val user1 = tEnv.sqlQuery("select * from user1 limit 2")   // 方式二

    // =====================向目标表插入数据=====================
    // user1.executeInsert("user2")     // 方式一
    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsert("user2", user1)    // 方式二
    // stmtSet.addInsertSql("insert into user2 select * from user1 limit 2")   // 方式三
    stmtSet.execute()

  }

}
