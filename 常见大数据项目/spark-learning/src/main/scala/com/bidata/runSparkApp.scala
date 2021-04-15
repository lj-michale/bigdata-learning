package com.bidata


import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import com.bidata.factory.{Dept, Factory}
import com.bidata.common.ConfigReader.readFromLocal
import com.bidata.common.ConfigParser.parseJsonStringFromLocal


object runSparkApp {

  def main(args: Array[String]): Unit = {

    val job_name = args(0)
    val btime = args(1)
    val etime = args(2)
    val product_json = parseJsonStringFromLocal("/Users/aaronpr_us/Documents/Academics/Job/JG/Project/cvr/src/main/resources/conf/ProductInfo.json")
    val config = readFromLocal("/Users/aaronpr_us/Documents/Academics/Job/JG/Project/cvr/src/main/resources/conf/StatCtr.conf")

    println(" Scala 学习开始 ")

    val dept:Dept = Class.forName("com.bidata.factory." + job_name + "Factory")
      .getDeclaredConstructor(classOf[Config], classOf[Map[String, List[Map[String, String]]]])
      .newInstance(config, product_json)
      .asInstanceOf[Factory]
      .getDept()


  }

}
