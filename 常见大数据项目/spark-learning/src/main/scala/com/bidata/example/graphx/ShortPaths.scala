package com.bidata.example.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.lib.ShortestPaths

object ShortPaths {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ShortPaths").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 测试的真实结果，后面用于对比
    val shortestPaths = Set(
      (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
      (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))

    // 构造无向图的边序列
    val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
      case e => Seq(e, e.swap)
    }

    // 构造无向图
    val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
    val graph = Graph.fromEdgeTuples(edges, 1)

    // 要求最短路径的点集合
    val landmarks = Seq(1, 4).map(_.toLong)

    // 计算最短路径
    val results = ShortestPaths.run(graph, landmarks).vertices.collect.map {
      case (v, spMap) => (v, spMap.mapValues(i => i))
    }

    val shortestPath1 = ShortestPaths.run(graph, landmarks)
    // 与真实结果对比
    println("\ngraph edges");
    println("edges:");
    graph.edges.collect.foreach(println)
    //    graph.edges.collect.foreach(println)
    println("vertices:");
    graph.vertices.collect.foreach(println)
    //    println("triplets:");
    //    graph.triplets.collect.foreach(println)
    println();

    println("\n shortestPath1");
    println("edges:");
    shortestPath1.edges.collect.foreach(println)
    println("vertices:");
    shortestPath1.vertices.collect.foreach(println)
    //    println("vertices:")

    assert(results.toSet == shortestPaths)
    println("results.toSet:" + results.toSet);
    println("end");

    val myVertices = sc.makeRDD(Array((1L, "A"),
      (2L, "B"),
      (3L, "C"),
      (4L, "D"),
      (5L, "E"),
      (6L, "F"),
      (7L, "G"))
    )

    val myEdges = sc.makeRDD(Array(
      Edge(1L, 2L, 7.0),
      Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0),
      Edge(2L, 4L, 9.0),
      Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0),
      Edge(4L, 5L, 15.0),
      Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0),
      Edge(5L, 7L, 9.0),
      Edge(6L, 7L, 11.0)))

    val myGraph = Graph(myVertices, myEdges)
    dijkstra(myGraph, 1L).vertices.map(_._2).collect

    sc.stop()
  }

  // Scala中的Dijstra最短路径算法
  def dijkstra[VD](g:Graph[VD,Double], origin:VertexId): Graph[(VD,Double), Double] = {
    /**
     * 1. 初始化
     * 遍历图的所有节点
     * 变为(false, Double.MaxValue的形式，后者是初始化的距离)
     * 如果是origin节点，则变为0
     */
    var g2 = g.mapVertices((vid,vd) => (false, if (vid == origin) 0 else Double.MaxValue))

    /**
     * 2. 遍历所有的点，找到最短路径的点，并作为当前顶点
     */
    for (i <- 1L to g.vertices.count-1) {
      val currentVertexId = g2.vertices.filter(!_._2._1).fold((0L,(false,Double.MaxValue)))((a,b) => if (a._2._2 < b._2._2) a else b)._1
      // 3. 向与当前顶点相邻的顶点发消息，再聚合消息：取小值作为最短路径
      val newDistances: VertexRDD[Double] = g2.aggregateMessages[Double](
        // sendMsg: 向邻边发送消息，内容为边的距离与最短路径值之和
        ctx => if (ctx.srcId == currentVertexId) ctx.sendToDst(ctx.srcAttr._2 + ctx.attr),
        // mergeMsg: 选择较小的值为当前顶点的相邻顶点的最短路径值
        (a,b) => math.min(a,b))
      // 4. 生成结果图
      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) =>
        (vd._1 || vid == currentVertexId, math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
    }
    g.outerJoinVertices(g2.vertices)((vid, vd, dist) => (vd, dist.getOrElse((false,Double.MaxValue))._2))
  }

}
