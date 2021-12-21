package com.luoj.task.learn

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Skeleton code for the Out of order case.
 */
object OutOfOrderCase {

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.addSource(new SourceFunction[(String, Long)]() {
      def run(ctx: SourceFunction.SourceContext[(String, Long)]) {
        ctx.collect("key", 0L)
        ctx.collect("key", 1000L)
        ctx.collect("key", 2000L)
        ctx.collect("key", 3000L)
        ctx.collect("key", 3000L)
        ctx.collect("key", 4000L)
        ctx.collect("key", 5000L)
        // out of order
        ctx.collect("key", 4000L)
        ctx.collect("key", 6000L)
        ctx.collect("key", 6000L)
        ctx.collect("key", 7000L)
        ctx.collect("key", 8000L)
        ctx.collect("key", 10000L)
        // out of order
        ctx.collect("key", 8000L)
        ctx.collect("key", 9000L)

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }
      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long)] {

      //      private val outOfOrder = 0
      // Result
      // (key,13000)
      // (key,32000)
      // (key,10000)

      private val outOfOrder = 3000
      // Result
      // (key,17000)
      // (key,49000)
      // (key,10000)

      override def extractTimestamp(element: (String, Long), previousTimestamp: Long): Long = {
        element._2
      }

      def checkAndGetNextWatermark(lastElement: (String, Long), extractedTimestamp: Long): Watermark = {
        val ts = lastElement._2 - outOfOrder
        new Watermark(ts)
      }
    }).keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
      .sum(1).print()

    env.execute("Out of order")
  }
}