package com.bigdata.task.learn.cep

import java.util
import com.luoj.common.PropConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * @descr
 * 匹配后跳过策略：
 * input : b1 b2 b3 c
 * NO_SKIP ： b1 b2 b3 c / b2 b3 c / b3 c # 一次只跳过一个事件，就开始匹配
 * SKIP_TO_NEXT : b1 b2 b3 c / b2 b3 c / b3 c # 调到下一个 开始事件（也就是 b）
 * SKIP_PAST_LAST_EVENT : b1 b2 b3 c # 跳过所有匹配过的事件
 * SKIP_TO_FIRST[b] : b1 b2 b3 c / b2 b3 c / b3 c # 跳到第一个b（如果 第一个就是 b，从这个 b 后面的第一个b 开始）？
 * SKIP_TO_LAST[b] : b1 b2 b3 c / b3 c # 跳到最后一个b，如果模式里面没有连续的b， 应该是调到 c 的后一个事件 ？
 * @date
 */

object AfterMatchStrategyDemo {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
    val topic = "match_strategy"
    val source = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), PropConfigUtil.getProperties)

    val input = env.addSource(source).map(str => {
        logger.info(str)
        val arr = str.split(",")
        val id = arr(0)
        val name = arr(1)
        CepDemoEvent(id, 0, name, 0)
    }).setParallelism(1).keyBy(_.id) //  Applying your pattern on a non-keyed stream will result in a job with parallelism equal to 1

    /**
      * 模式说明：
      * 匹配价格连续上涨
      *
      * 匹配后跳过策略： 默认从上次的开始事件后的下一个事件开始
      *
      * NO_SKIP：default
      * SKIP_TO_NEXT：
      * SKIP_PAST_LAST_EVENT：
      * SKIP_TO_FIRST[b]：
      * SKIP_TO_LAST[b]：
      *
      */
    val noSkit = AfterMatchSkipStrategy.noSkip()
    val pattern = Pattern.begin[CepDemoEvent]("first").where(event => {
      event.name.equals("a")
    })
      //      .timesOrMore(1)
      .next("second").where(event => {
      event.name.equals("a")
    })
      .next("third").where(event => {
      event.name.equals("b")
    })
    //.notNext()
    // always remember add within, it will reduce the state usage
    //      .within(Time.minutes(5 * 60 * 1000))
    val patternStream = CEP.pattern(input, pattern)

    val result: DataStream[String] = patternStream.process(
      new PatternProcessFunction[CepDemoEvent, String]() {
        override def processMatch( events: util.Map[String, util.List[CepDemoEvent]],
                                   ctx: PatternProcessFunction.Context,
                                   out: Collector[String] ): Unit = {
          // get the change
          val first = events.get("first").get(0)
          val second = events.get("second").get(0)
          val third = events.get("third").get(0)

          out.collect("first : " + first + ", first " + second + ", third : " + third)

        }
      }
    )

    // for convenient, just print
    result.print()

    env.execute(this.getClass.getName)

  }

  case class CepDemoEvent(id: String, volume: Int, name: String, num: Int)

}
