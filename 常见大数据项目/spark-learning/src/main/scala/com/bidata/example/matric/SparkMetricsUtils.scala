package com.bidata.example.matric

import com.alibaba.fastjson.JSON

/**
 * Spark监控数据获取工具类
 */
object SparkMetricsUtils {

  /**
   * 获取监控页面信息
   */
  def getMetricsJson(url: String,
                     connectTimeout: Int = 5000,
                     readTimeout: Int = 5000,
                     requestMethod: String = "GET") ={

    import java.net.{HttpURLConnection, URL}

    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)

    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if(inputStream != null) inputStream.close()

    JSON.parseObject(content)

  }
}

