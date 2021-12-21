package com.bigdata.task.learn.extensions

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions

object Main {

  case class Point(x: Double, y: Double)

  def main(args: Array[String]): Unit = {

//    val env = ExecutionEnvironment.getExecutionEnvironment
//
//    import org.apache.flink.api.scala._
//    val ds: DataSet[Point] = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
//
//    ds.filterWith {
//      case Point(x, _) => x > 1
//    }.reduceWith {
//      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
//    }.mapWith {
//      case Point(x, y) => (x, y)
//    }.flatMapWith {
//      case (x, y) => Seq("x" -> x, "y" -> y)
//    }.groupingBy {
//      case (id, value) => id
//    }

  }



}
