package com.bigdata.task.learn.flinksql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row


object FlinkKafkaDDLDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 设置运行
    env.setParallelism(3)

    val createTable =
      """
        |CREATE TABLE PERSON (
        |    name VARCHAR COMMENT '姓名',
        |    age VARCHAR COMMENT '年龄',
        |    city VARCHAR COMMENT '所在城市',
        |    address VARCHAR COMMENT '家庭住址',
        |    ts TIMESTAMP COMMENT '时间戳'
        |)
        |WITH (
        |    'connector.type' = 'kafka', -- 使用 kafka connector
        |    'connector.version' = '0.11',  -- kafka 版本
        |    'connector.topic' = 'quick_app_msglifecycle',  -- kafka topic
        |    'connector.startup-mode' = 'latest-offset', -- 从最新的 offset 开始读取
        |    'connector.properties.0.key' = 'zookeeper.connect',  -- 连接信息
        |    'connector.properties.0.value' = 'xxx',
        |    'connector.properties.1.key' = 'bootstrap.servers',
        |    'connector.properties.1.value' = 'localhost:9092',
        |    'update-mode' = 'append',
        |    'format.type' = 'json',  -- 数据源格式为 json
        |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
        |)
        """.stripMargin

    tEnv.sqlUpdate(createTable)

    val query =
      """
        |SELECT name,COUNT(age) FROM PERSON GROUP BY name
        |""".stripMargin

    val result = tEnv.sqlQuery(query)

//    result.toRetractStream[Row].print()



    tEnv.execute("Flink SQL DDL")


  }


}
