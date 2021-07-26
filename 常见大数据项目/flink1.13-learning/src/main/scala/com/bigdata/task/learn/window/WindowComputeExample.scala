package com.bigdata.task.learn.window

import java.time.Duration
import java.util.{Random, UUID}

import com.bigdata.task.learn.flinksql.FlinkSQLExample0003.MyExampleSource
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.java.tuple.Tuple6
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author lj.michale
 * @description WindowComputeExample
 *              窗口 生命周期
 *              Keyed vs Non-Keyed 窗口
 *              窗口分配器
 *                滚动窗口
 *                滑动窗口
 *                会话窗口
 *                全局窗口
 *              窗口函数
 *                ReduceFunction
 *                FoldFunction
 *                窗口函数 - 通用场景
 *                ProcessWindowFunction
 *                窗口函数与增量聚合结合
 *              触发器
 *                触发和清除
 *                窗口分配器的默认触发器
 *                内置触发器和自定义触发器
 *              驱逐器（Evictors）
 *              允许的延迟（Allowed Lateness）
 *                把晚到元素当做side output
 *                晚到元素的考虑
 *              有用状态大小的考虑
 * @date 2021-07-20
 */
object WindowComputeExample {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")
    env.setStateBackend(new EmbeddedRocksDBStateBackend)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    import org.apache.flink.api.scala._
    val inputDS: DataStream[order] = env.addSource(new MyExampleSource)

    // 生成水印watermark
//    1.assignTimestamps(extractor: TimestampExtractor[T]): DataStream[T]
//    此方法是数据流的快捷方式，其中已知元素时间戳在每个并行流中单调递增。在这种情况下，系统可以通过跟踪上升时间戳自动且完美地生成水印。
//    inputDS.assignAscendingTimestamps(element => element.buyTime).print()

//    inputDS.assignTimestampsAndWatermarks(
//      new WatermarkStrategy[order] {
//        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[order] = {
//
//        }
//      }
//    )



//    val winDS:DataStream[order] = inputDS.keyBy(0)
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .reduce{
//      (row1, row2) => order(row1.userId, row1.productName, row1.amount + row2.amount, row1.buyTime)
//    }
//    winDS.print()


    env.execute("WindowComputeExample")

  }


  /**
   * @descr 自定义source数据集
   */
  class MyExampleSource() extends SourceFunction[order] {

    var running = true
    var timestamp = System.currentTimeMillis()

    /**
     * @descr run
     * @param sourceContext
     */
    override def run(sourceContext: SourceContext[order]): Unit = {
      while (running) {
        val random: Random = new Random()
        val userId:String = getRandomUserId()
        val productName:String = getRandomProductName(random)
        val amount:Double = getRandomAmount(random)
        Thread.sleep(1000)
        sourceContext.collect(order(userId, productName, amount, timestamp))
      }
    }

    override def cancel(): Unit = {
      running = false
    }

    /**
     * @descr 产生随机UserId
     */
    def getRandomUserId(): String = {
      UUID.randomUUID.toString.replaceAll("-", "")
    }

    /**
     * @descr 产生随机ProductName
     */
    def getRandomProductName(random: Random): String = {
      val array = Array("欢乐薯条", "衣服", "鞋子", "J帽子", "《安徒生童话全集》", "蜂蜜", "盆景", "《SpringCloud微服务架构设计与开发》","RubberOK")
      array(random.nextInt(array.length))
    }

    /**
     * @descr 产生随机amount
     */
    def getRandomAmount(random: Random): Double = {
      val array = Array(18.23, 5.45, 46.78, 98.12, 976.33, 234.76, 82.12, 107.25, 180.34)
      array(random.nextInt(array.length))
    }

  }

  case class order(userId:String, productName:String, amount:Double, buyTime:Long)

}
