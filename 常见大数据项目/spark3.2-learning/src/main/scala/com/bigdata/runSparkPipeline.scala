package com.bigdata

import java.util.Properties

import com.turing.common.PropertiesUtils
import org.apache.log4j.Logger

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-05-20
 */
object runSparkPipeline {

  val logger = Logger.getLogger(runSparkPipeline.getClass)

  def main(args: Array[String]): Unit = {

    val pipelineProps = PropertiesUtils.getProperties("spark-dev.properties")


  }


}
