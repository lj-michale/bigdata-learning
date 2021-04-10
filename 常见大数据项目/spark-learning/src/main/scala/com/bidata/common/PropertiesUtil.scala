package com.bidata.common

import java.util.Properties

/**
 * Properties的工具类
 */
object PropertiesUtil {

  private val properties: Properties = new Properties

  /**
   *
   * 获取配置文件Properties对象
   *
   * @author yore
   * @return java.util.Properties
   */
  def getProperties() :Properties = {
    if(properties.isEmpty){
      //读取源码中resource文件夹下的my.properties配置文件
      val reader = getClass.getResourceAsStream("/my.properties")
      properties.load(reader)
    }
    properties
  }

  /**
   *
   * 获取配置文件中key对应的字符串值
   *
   * @author yore
   * @return java.util.Properties
   */
  def getPropString(key : String) : String = {
    getProperties().getProperty(key)
  }

  /**
   *
   * 获取配置文件中key对应的整数值
   *
   * @author yore
   */
  def getPropInt(key : String) : Int = {
    getProperties().getProperty(key).toInt
  }

  /**
   *
   * 获取配置文件中key对应的布尔值
   *
   * @return java.util.Properties
   */
  def getPropBoolean(key : String) : Boolean = {
    getProperties().getProperty(key).toBoolean
  }

}