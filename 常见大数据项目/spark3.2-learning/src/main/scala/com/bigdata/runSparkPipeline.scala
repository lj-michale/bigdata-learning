package com.bigdata

import java.util.Properties

import com.turing.common.PropertiesUtils
import org.apache.log4j.Logger


object runSparkPipeline {

  val logger = Logger.getLogger(runSparkPipeline.getClass)

  def main(args: Array[String]): Unit = {

    val hotleProperties: Properties = PropertiesUtils.getProperties("spark-dev.properties")


  }


}
