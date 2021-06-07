package com.bidata.example.factory

import com.typesafe.config.Config

trait Factory {

  // attributes
  def config: Config
  def product_json: Map[String, List[Map[String, String]]]

  // methods
  def getDept(): Dept
}
