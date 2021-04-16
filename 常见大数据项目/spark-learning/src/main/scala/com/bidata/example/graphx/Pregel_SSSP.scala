package com.bidata.example.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, VertexId, Graph}
import org.apache.spark.graphx.util.GraphGenerators

object Pregel_SSSP {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Pregel_SSSP")
    val sc = new SparkContext(conf)

    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] = GraphGenerators.logNormalGraph(sc, numVertices = 5).mapEdges(e => e.attr.toDouble)
    graph.edges.foreach(println)
    val sourceId: VertexId = 0 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph : Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[VertexId](sourceId))
      else (Double.PositiveInfinity, List[VertexId]()))

    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(
      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,
      // Send Message
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)
    println(sssp.vertices.collect.mkString("\n"))
  }

}