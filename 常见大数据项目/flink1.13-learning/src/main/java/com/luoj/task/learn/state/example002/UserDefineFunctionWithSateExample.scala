package com.luoj.task.learn.state.example002

import com.luoj.task.learn.source.{SensorReading, SensorSource, ThresholdUpdate}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * @descr Flink状态编程：state
 *        实现有状态的用户自定义函数
 *        函数有两种状态，键控状态(keyed state)和操作符状态(operator state)。
 *        1.实现有状态的用户自定义函数
 *          1.1 在RuntimeContext中定义键控状态
 *          1.2 使用ListCheckpointed接口来实现操作符的列表状态
 *          1.3 使用连接的广播状态
 *        2 配置检查点
 *          2.1 将hdfs配置为状态后端
 *        3 保证有状态应用的可维护性
 *          3.1 指定唯一的操作符标识符
 *          3.2 指定操作符的最大并行度
 *        4 有状态应用的性能和健壮性
 *          4.1 选择一个状态后端
 *          4.2 防止状态泄露
 *
 * @author lj.michale
 * @date 2021/6/23 10:24
 *
 */
object UserDefineFunctionWithSateExample {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, org.apache.flink.api.common.time.Time.seconds(10), org.apache.flink.api.common.time.Time.seconds(1)))
    env.getConfig.setAutoWatermarkInterval(5000L)  // 每5秒发出一个watermark
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    env.setParallelism(1)
    import org.apache.flink.api.scala._

    val inputStream: DataStream[SensorReading] = env.addSource(new SensorSource)
    val keyedData: KeyedStream[SensorReading, String] = inputStream.keyBy(_.id)

    // 实现有状态的用户自定义函数
    /**
     * 用户自定义函数可以使用keyed state来存储和访问key对应的状态。
     * 对于每一个key，Flink将会维护一个状态实例。
     * 一个操作符的状态实例将会被分发到操作符的所有并行任务中去。
     * 这表明函数的每一个并行任务只为所有key的某一部分key保存key对应的状态实例。
     * 所以keyed state和分布式key-value map数据结构非常类似。
     */
   // 方式1
//    val alertStream: DataStream[(String, Double, Double)] = keyedData
//      .flatMap(new TemperatureAlertFunction(10))
//    alertStream.print(">>>>>>>>>>>>> 连续两次温度相差10度").setParallelism(1)

    // 方式2
    // 使用FlatMap with keyed ValueState的快捷方式flatMapWithState也可以实现以上需求。
    val alertStream: DataStream[(String, Double, Double)] = keyedData
      .flatMapWithState[(String, Double, Double), Double] {
        case (in: SensorReading, None) =>
          // no previous temperature defined.
          // Just update the last temperature
          (List.empty, Some(in.timepreture))
        case (r: SensorReading, lastTemp: Some[Double]) =>
          // compare temperature difference with threshold
          val tempDiff = (r.timepreture - lastTemp.get).abs
          if (tempDiff > 10) {
            // threshold exceeded.
            // Emit an alert and update the last temperature
            (List((r.id, r.timepreture, tempDiff)), Some(r.timepreture))
          } else {
            // threshold not exceeded. Just update the last temperature
            (List.empty, Some(r.timepreture))
          }
      }
    alertStream.print(">>>>>>>>>>>>> 连续两次温度相差10度").setParallelism(1)

    // 1.2 使用ListCheckpointed接口来实现操作符的列表状态
    // 操作符状态会在操作符的每一个并行实例中去维护。一个操作符并行实例上的所有事件都可以访问同一个状态。Flink支持三种操作符状态：list state, list union state, broadcast state。


    // 1.3 使用连接的广播状态
    // 一个常见的需求就是流应用需要将同样的事件分发到操作符的所有的并行实例中，而这样的分发操作还得是可恢复的。
    // 我们举个例子：一条流是一个规则(比如5秒钟内连续两个超过阈值的温度)，另一条流是待匹配的流。
    // 也就是说，规则流和事件流。所以每一个操作符的并行实例都需要把规则流保存在操作符状态中。也就是说，规则流需要被广播到所有的并行实例中去。
    // 在Flink中，这样的状态叫做广播状态(broadcast state)。广播状态和DataStream或者KeyedStream都可以做连接操作。
    // 下面的例子实现了一个温度报警应用，应用有可以动态设定的阈值，动态设定通过广播流来实现。
    // the descriptor of the broadcast state
    val broadcastStateDescriptor: MapStateDescriptor[String, Double] = new MapStateDescriptor[String, Double]("thresholds",
      classOf[String], classOf[Double])
    //val broadcastThresholds: BroadcastStream[ThresholdUpdate] = alertStream.broadcast(broadcastStateDescriptor)

    // connect keyed sensor stream and broadcasted rules stream
    //val alerts: DataStream[(String, Double, Double)] = keyedData
    //  .connect(broadcastThresholds).process(new UpdatableTemperatureAlertFunction())


    env.execute(this.getClass.getName)

  }

  /**
   * 该例子中，每一个并行实例都计数了本实例有多少温度值超过了设定的阈值。
   * 例子中使用了操作符状态，并且每一个并行实例都拥有自己的状态变量，这个状态变量将会被检查点操作保存下来，
   * 并且可以通过使用ListCheckpointed接口来恢复状态变量。
   */
  class TemperatureAlertFunction(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    private var lastTempState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
      lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
    }

    override def flatMap(reading: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      val lastTemp = lastTempState.value()
      val tempDiff = (reading.timepreture - lastTemp).abs

      if (tempDiff > threshold) {
        out.collect((reading.id, reading.timepreture, tempDiff))
      }

      this.lastTempState.update(reading.timepreture)
    }

  }

  // 业务场景为：一个对每一个并行实例的超过阈值的温度的计数程序
  // 使用ListCheckpointed接口来实现操作符的列表状态
  // 每一个并行实例都计数了本实例有多少温度值超过了设定的阈值。
  // 例子中使用了操作符状态，并且每一个并行实例都拥有自己的状态变量，这个状态变量将会被检查点操作保存下来，并且可以通过使用ListCheckpointed接口来恢复状态变量。
  class HighTempCounter(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (Int, Long)]
      with ListCheckpointed[java.lang.Long] {

    // index of the subtask
    private lazy val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask

    // local count variable
    private var highTempCnt = 0L

    override def flatMap( in: SensorReading, out: Collector[(Int, Long)]): Unit = {
      if (in.timepreture > threshold) {
        // increment counter if threshold is exceeded
        highTempCnt += 1
        // emit update with subtask index and counter
        out.collect((subtaskIdx, highTempCnt))
      }
    }

    override def restoreState(state: java.util.List[java.lang.Long]): Unit = {
      highTempCnt = 0
      // restore state by adding all longs of the list
      for (cnt <- state.asScala) {
        highTempCnt += cnt
      }
    }

//    override def snapshotState(chkpntId: Long, ts: Long): java.util.List[java.lang.Long] = {
//      // snapshot state as list with a single count
//      java.util.Collections.singletonList(highTempCnt)
//    }

    // 我们可以看到操作符的每一个并行实例都暴露了一个状态对象的列表。如果我们增加操作符的并行度，那么一些并行任务将会从0开始计数。
    // 为了获得更好的状态分区的行为，当HighTempCounter函数扩容时，我们可以按照下面的程序来实现snapshotState()方法，这样就可以把计数值分配到不同的并行计数中去了。
    override def snapshotState( chkpntId: Long, ts: Long): java.util.List[java.lang.Long] = {
      // split count into ten partial counts
      val div = highTempCnt / 10
      val mod = (highTempCnt % 10).toInt
      // return count as ten parts
      (List.fill(mod)(new java.lang.Long(div + 1)) ++
        List.fill(10 - mod)(new java.lang.Long(div))).asJava
    }
  }

  class UpdatableTemperatureAlertFunction()
    extends KeyedBroadcastProcessFunction[String,
      SensorReading, ThresholdUpdate, (String, Double, Double)] {

    // the descriptor of the broadcast state
    private lazy val thresholdStateDescriptor =
      new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

    // the keyed state handle
    private var lastTempState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      // create keyed state descriptor
      val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
      // obtain the keyed state handle
      lastTempState = getRuntimeContext
        .getState[Double](lastTempDescriptor)
    }

    override def processBroadcastElement( update: ThresholdUpdate,
                                          ctx: KeyedBroadcastProcessFunction[String,
                                            SensorReading, ThresholdUpdate,
                                            (String, Double, Double)]#Context,
                                          out: Collector[(String, Double, Double)]): Unit = {
      // get broadcasted state handle
      val thresholds = ctx.getBroadcastState(thresholdStateDescriptor)
      if (update.threshold != 0.0d) {
        // configure a new threshold for the sensor
        thresholds.put(update.id, update.threshold)
      } else {
        // remove threshold for the sensor
        thresholds.remove(update.id)
      }
    }

    override def processElement(reading: SensorReading,
                                readOnlyCtx: KeyedBroadcastProcessFunction
                                     [String, SensorReading, ThresholdUpdate,
                                     (String, Double, Double)]#ReadOnlyContext,
                                out: Collector[(String, Double, Double)]): Unit = {
      // get read-only broadcast state
      val thresholds = readOnlyCtx.getBroadcastState(thresholdStateDescriptor)
      // check if we have a threshold
      if (thresholds.contains(reading.id)) {
        // get threshold for sensor
        val sensorThreshold: Double = thresholds.get(reading.id)
        // fetch the last temperature from state
        val lastTemp = lastTempState.value()
        // check if we need to emit an alert
        val tempDiff = (reading.timepreture - lastTemp).abs
        if (tempDiff > sensorThreshold) {
          // temperature increased by more than the threshold
          out.collect((reading.id, reading.timepreture, tempDiff))
        }
      }

      // update lastTemp state
      this.lastTempState.update(reading.timepreture)
    }
  }




}
