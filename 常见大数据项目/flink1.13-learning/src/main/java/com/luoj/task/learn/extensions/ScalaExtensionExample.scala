package com.luoj.task.learn.extensions

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.scala.extensions._

object ScalaExtensionExample {

  case class Point(x: Double, y: Double)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))

    ds.filterWith {
      case Point(x, _) => x > 1
    }.mapWith {
      case Point(x, y) => (x, y)
    }.flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y)
    }.keyingBy {
      case (id, value) => id
    }

  }
}
