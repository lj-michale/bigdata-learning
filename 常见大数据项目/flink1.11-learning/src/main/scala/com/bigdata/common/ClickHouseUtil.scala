package com.bigdata.common

import java.sql.{Connection, DriverManager}

object ClickHouseUtil {

  def getConnection(ip:String, port:Int, tableName:String): Connection ={
    var connection: Connection = null
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    connection = DriverManager.getConnection("jdbc:clickhouse://" + ip + ":" + port + "/" + tableName)
    connection
  }


}
