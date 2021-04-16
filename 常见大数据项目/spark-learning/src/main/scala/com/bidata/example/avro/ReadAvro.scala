package com.bidata.example.avro

import com.alibaba.fastjson.JSON
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}


object ReadAvro {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("log_deal").setMaster("local")
    val sc = new SparkContext(conf)
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](args(0))
    avroRDD.map{l=>
      val line = l._1.toString
      val json= JSON.parseObject(line)
      val shape=json.get("shape")
      val count=json.get("count")
      (shape,count)}.foreach(println)
    sc.stop()
  }

}
