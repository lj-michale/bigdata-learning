package com.bidata.example.udf.example001

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkSQLUDF4 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("udf")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.close()
  }

  /**
   * total : 总点击数量
   * String：城市名
   * Long：各个城市的点击数量
   */
  case class Buffer(var total:Long, var cityMap: mutable.Map[String, Long])

  /**
   * 自定义聚合函数，实现城市备注功能
   * 1、继承Aggregator定义泛型
   * // IN ： 城市名称
   * // Buffer： 【总点击量，Map[(city, cnt), (city, cnt)]】
   * // OUT: 备注信息
   */
  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {
    // 缓冲区初始化
    override def zero: Buffer = {
      Buffer(0, mutable.Map[String, Long]())
    }

    // 更新缓冲区数据
    override def reduce(buf: Buffer, city: String): Buffer = {
      buf.total += 1 // 点击数量+1
      val newCnt = buf.cityMap.getOrElse(city, 0L) + 1
      buf.cityMap.update(city, newCnt)
      buf
    }

    // 分布式计算合并缓冲区
    override def merge(buf1: Buffer, buf2: Buffer): Buffer = {
      buf1.total += buf2.total
      val map1: mutable.Map[String, Long] = buf1.cityMap
      val map2: mutable.Map[String, Long] = buf2.cityMap
      // 两个map的合并操作
      buf1.cityMap = map1.foldLeft(map2) {
        case (map, (city, cnt)) => {
          val newCnt = map.getOrElse(city, 0L) + cnt
          map.update(city, newCnt)
          map
        }
      }
      buf1

      // 方式2：两个map的合并操作
      //      map2.foreach{
      //        case (city, cnt) => {
      //          val newCnt = map1.getOrElse(city, 0L) + cnt
      //          map1.update(city, newCnt)
      //        }
      //      }
      //      buf1.cityMap = map1
      //      buf1
    }

    // 将统计结果生成统计信息
    override def finish(buf: Buffer): String = {
      val remarkList = ListBuffer[String]()
      val totalCnt = buf.total
      val cityMap = buf.cityMap
      // 降序排列
      val cityCntList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)
      var sumRadio = 0
      cityCntList.foreach{
        case (city, cnt) => {
          var radio = cnt * 100 / totalCnt
          remarkList.append(s"${city} ${radio}%")
          sumRadio += radio
        }
      }
      val hasMore = cityMap.size > 2
      if (hasMore) {
        remarkList.append(s"其他 ${100-sumRadio}%")
      }
      remarkList.mkString(", ")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product
    override def outputEncoder: Encoder[String] = Encoders.STRING
  }


}
