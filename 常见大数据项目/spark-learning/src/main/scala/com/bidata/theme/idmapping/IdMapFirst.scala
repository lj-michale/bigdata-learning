package com.bidata.theme.idmapping

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @descr 初始化One Id
 *
 * @date 2022/1/23 9:35
 */
object IdMapFirst {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("id-mapping")
      .master("local[1]")
      .getOrCreate()
    //将rdd变成df
    import spark.implicits._

    val rawData = spark.read.textFile("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark-learning\\data\\userInfo.json")
    val data: RDD[Array[String]] = rawData.rdd.map(line => {
      //将每行数据解析成json对象
      val jsonObj = JSON.parseObject(line)
      // 从json对象中取user对象
      //      val userObj = jsonObj.getJSONObject("user")
      val uid = jsonObj.getString("uid")
      // 从user对象中取phone对象
      val phoneObj = jsonObj.getJSONObject("phone")
      val imei = phoneObj.getOrDefault("imei","").toString
      val mac = phoneObj.getOrDefault("mac","").toString
      val imsi = phoneObj.getOrDefault("imsi","").toString
      val androidId = phoneObj.getOrDefault("androidId","").toString
      val deviceId = phoneObj.getOrDefault("deviceId","").toString
      val uuid = phoneObj.getOrDefault("uuid","").toString
      Array(uid, imei, mac, imsi, androidId, deviceId, uuid).filter(StringUtils.isNotBlank(_))
    }
    )

    val vertices: RDD[(Long, String)] = data.flatMap(arr => {
      for (id <- arr) yield (id.hashCode.toLong, id)
    })

    vertices.foreach(ele => println(ele._1 + " : " + ele._2))

    val edges: RDD[Edge[String]] = data.flatMap(arr => {
      for (i <- 0 to arr.length - 2; j <- i + 1 to arr.length - 1) yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
    }).map(edge => (edge, 1)).reduceByKey(_ + _)
      .filter(tp => tp._2 > 2)
      .map(x => x._1)

    //用 点集合 和 边集合 构造一张图  使用Graph算法
    val graph = Graph(vertices,edges)
    //并调用最大连同子图算法VertexRDD[VertexId] ==>rdd 里面装的元组(Long值,组中最小值)
    val res: VertexRDD[VertexId] = graph.connectedComponents().vertices
    val firstIds =   res.toDF("id","guid")

    firstIds.write.parquet("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark-learning\\data\\userIds_demo")

    spark.stop()
  }

}