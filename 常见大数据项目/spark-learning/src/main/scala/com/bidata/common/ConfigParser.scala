package com.bidata.common

import com.typesafe.config.Config

import collection.JavaConverters._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util
import scala.io._

object ConfigParser {

  def parseOneConfig(config: Config, path: String): String = {
    config.getString(path)
  }


  def parseAllConfig(config: Config): Map[String, String] = {
    var config_map = Map.empty[String, String]
    val entries = config.entrySet().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      config_map += (entry.getKey -> entry.getValue.unwrapped().toString)
    }
    config_map
  }


  def parseConfig(config: Config, name: String): Map[String, String] ={
    var config_map = Map.empty[String, String]
    val entries = config.getObject(name).asScala.iterator
    while (entries.hasNext) {
      val entry = entries.next()
      config_map += (entry._1 -> entry._2.unwrapped().toString)
    }
    config_map
  }

  def parseJsonString(json_str: String):Map[String, String] ={
    implicit val formats = DefaultFormats
    parse(json_str).extract[Map[String, String]]
  }


  def parseJsonStringFromLocal(json_path: String): Map[String, List[Map[String, String]]] ={
    implicit val formats = DefaultFormats
    parse(Source.fromFile(json_path).mkString).extract[Map[String, List[Map[String, String]]]]
  }


}


