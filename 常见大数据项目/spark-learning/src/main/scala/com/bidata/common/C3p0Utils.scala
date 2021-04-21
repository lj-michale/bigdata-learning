package com.bidata.common

import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
 * @author :
 * @date :
 */
object C3p0Utils {
  def getDataSource(dbOptions: Map[String, String]): ComboPooledDataSource
  = {
    val properties = new Properties()
    dbOptions.foreach(x => properties.setProperty(x._1, x._2))
    val dataSource = new ComboPooledDataSource()
    dataSource.setDriverClass(dbOptions("driverClass"))
    dataSource.setJdbcUrl(dbOptions("jdbcUrl"))
    dataSource.setProperties(properties)
    dataSource
  }

}