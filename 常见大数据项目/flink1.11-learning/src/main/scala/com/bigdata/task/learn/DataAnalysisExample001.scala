package com.bigdata.task.learn

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.types.Row


object DataAnalysisExample001 {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)
//    val dbData = env.createInput(JDBCInputFormat
//                                 .buildJDBCInputFormat.setDrivername("com.mysql.cj.jdbc.Driver")
//                                 .setDBUrl("jdbc:mysql://localhost:3306/analysis?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
//      .setUsername("root")
//      .setPassword("abc1314520")
//      .setQuery("select * from tianchi_mobile_recommend_train_user")
//      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)).finish)



  }



}
