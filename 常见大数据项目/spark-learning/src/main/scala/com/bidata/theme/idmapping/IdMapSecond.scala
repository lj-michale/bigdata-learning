package com.bidata.theme.idmapping

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object IdMapSecond {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("id-mapping")
      .master("local[1]")
      .getOrCreate()

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
    })

    val vertices: RDD[(Long, String)] = data.flatMap(arr => {
      for (id <- arr) yield (id.hashCode.toLong, id)
    })

    vertices.foreach(ele => println(ele._1 + " : " + ele._2))

    val edges: RDD[Edge[String]] = data.flatMap(arr => {
      //用双重for循环的方法让数组中所有的两两组合成边
      for (i <- 0 to arr.length - 2; j <- i + 1 to arr.length - 1) yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
    }).map(edge => (edge, 1)).reduceByKey(_ + _)
      //过滤将重复次数<5(经验阈值)的边去掉,
      .filter(tp => tp._2 > 2)
      .map(x => x._1)

    //从初次的guid读取
    val firstIdmap = spark.read.parquet("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark-learning\\data\\userIds_demo2")
    val firstVertices = firstIdmap.rdd.map({
        case Row(id_hashcode: VertexId, guid: VertexId) =>
          (id_hashcode, "")
      })

    val firstEdges = firstIdmap.rdd.map(row => {
      val id_hashcode = row.getAs[VertexId]("id")
      val guid = row.getAs[VertexId]("guid")
      Edge(id_hashcode,guid,"")
    })

    // 通过vertex, edge沟通graph
    val graph = Graph(vertices.union(firstVertices),edges.union(firstEdges))
    //result:  VertexRDD[VertexId] => rdd(点id-long, 组中最小值)
    val result:  VertexRDD[VertexId] = graph.connectedComponents().vertices

    val idMap = firstIdmap.rdd.map(row => {
      val id_hashcode = row.getAs[VertexId]("id")
      val guid = row.getAs[VertexId]("guid")
      (id_hashcode,guid)
    }).collectAsMap()

    val bcMap = spark.sparkContext.broadcast(idMap)

    import spark.implicits._

    val todayIdmap = result.map(tup => (tup._2,tup._1))
      .groupByKey()
      .mapPartitions( iter => {
        iter.map(tup => {
          val idmapMap = bcMap.value
          var todayGuid = tup._1
          val ids = tup._2
          //遍历id，挨个映射查找
          var idFind = false
          for (id <- ids if !idFind) {
            val getGuid = idmapMap.get(id)
            if (getGuid.isDefined) {
              todayGuid = getGuid.get
              idFind = true
            }
          }
          (todayGuid, ids)
        })
      }).flatMap(tup => {
        val ids = tup._2
        val guid = tup._1
        for(ele <- ids) yield (ele, guid)
      }).toDF("id", "guid")

    todayIdmap.show()
    todayIdmap.createOrReplaceTempView("id_guid");

    val data2 = data.flatMap( arr => {
      for(id <- arr) yield (id.hashCode.toLong, id)
    }).toDF("id", "str_id")

    data2.createOrReplaceTempView("id_original")

    val output = spark.sql("""select
                             |    t1.id as id_hashcode,
                             |    t2.str_id as id,
                             |    t1.guid as guid
                             |from id_guid t1
                             |left join id_original t2 on t1.id = t2.id
                             |group by t1.id, t2.str_id, t1.guid
                             |order by guid""".stripMargin
    )
    output.show()

  }

}