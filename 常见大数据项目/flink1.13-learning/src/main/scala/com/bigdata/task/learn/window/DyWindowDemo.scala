package com.bigdata.task.learn.window

import java.text.SimpleDateFormat
import java.util
import java.util.{Properties, Random}

import com.google.gson.Gson
import com.luoj.task.learn.window.DynamicTumblingEventTimeWindows
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * @author lj.michale
 * @description
 * @date 2021-07-16
 */
object DyWindowDemo {

  private val logger = LoggerFactory.getLogger(DyWindowDemo.getClass)

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

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "event")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val commandState = new MapStateDescriptor[String, Command]("commandState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[Command]() {}))

    val kafkaConsumerSource = new FlinkKafkaConsumer[String]("topic_name", new SimpleStringSchema(), properties)

    val commandStream = env.addSource(kafkaConsumerSource).flatMap(new RichFlatMapFunction[String, Command] {
      var gson: Gson = _

      override def open(parameters: Configuration): Unit = {
        gson = new Gson()
      }

      override def flatMap(element: String, out: Collector[Command]): Unit = {
        try {
          val command = gson.fromJson(element, classOf[Command])

          if (command != null) {
            out.collect(command)
          }
        } catch {
          case e: Exception =>
            logger.warn("parse command error : " + element, e)
        }
      }
    }).broadcast(commandState)

    val dataStream = env.addSource(new DataSourceFunction).flatMap(new RichFlatMapFunction[String, DataEntity] {
        var gson: Gson = _

        override def open(parameters: Configuration): Unit = {
          gson = new Gson()
        }

        override def flatMap(element: String, out: Collector[DataEntity]): Unit = {
          try {
            val data = gson.fromJson(element, classOf[DataEntity])
            if (data != null) {
              out.collect(data)
            }
          } catch {
            case e: Exception =>
              logger.warn("parse input data error: {}" + element, e)
          }
        }
      })

    // connect stream
    val connectStream = dataStream
      .keyBy(_.attr)
      .connect(commandStream)
      .process(new KeyedBroadcastProcessFunction[String, DataEntity, Command, (DataEntity, Command)]() {
        // 存放当前命令的 map
        var currentCommand: MapState[String, Command] = _
        // 存放新命令的 map
        var commandState: MapStateDescriptor[String, Command] = _

        override def open(parameters: Configuration): Unit = {
          currentCommand = getRuntimeContext.getMapState(new MapStateDescriptor[String, Command]("currentCommand", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[Command]() {})))
          commandState = new MapStateDescriptor[String, Command]("commandState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint[Command]() {}))
        }

        override def processElement(element: DataEntity, ctx: KeyedBroadcastProcessFunction[String, DataEntity, Command, (DataEntity, Command)]#ReadOnlyContext, out: Collector[(DataEntity, Command)]): Unit = {
          // 命令可以是大于/小于当前时间
          // 小于当前时间的，直接添加即可,之前命令的窗口不会收到新数据，新数据直接进新命令的窗口
          // 大于当前时间的命令，不能直接与流一起往下游输出，等时间小于当前的 processTime 时间后，才会开始新窗口
          val command = ctx.getBroadcastState(commandState).get(element.attr)
          val current = currentCommand.get(element.attr)
          if (command != null && command.startTime <= ctx.currentProcessingTime()) {
            // 当新命令的时间小于当前的处理时间，替换旧命令
            currentCommand.put(element.attr, command)
          }
          // 如果当前命令为空，数据就不往下发送了
          if (current != null) {
            out.collect((element, current))
          }
          // command not exists, ignore it
        }

        override def processBroadcastElement(element: Command, ctx: KeyedBroadcastProcessFunction[String, DataEntity, Command, (DataEntity, Command)]#Context, out: Collector[(DataEntity, Command)]): Unit = {
          // only one command are new accepted, cover old command
          logger.info("receive command : " + element)
          ctx.getBroadcastState(commandState).put(element.targetAttr, element)
        }

      }).assignAscendingTimestamps(_._1.time)

    // todo process sum
    val sumStream = connectStream
      .keyBy(_._1.attr)
      .window(DynamicTumblingEventTimeWindows.of())
      .process(new DyProcessWindowFunction())
      .print("result:")

    env.execute(this.getClass.getName)

  }


  class DataSourceFunction extends SourceFunction[String] {

    var flag = true

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      var map = new util.HashMap[String, String]

      while (flag) {
        val random = new Random()
        val gson = new Gson()
        for (i <- 1 to 4) {
          map.put("attr", "attr" + i)
          map.put("value", "" + random.nextInt(1000))
          map.put("time", "" + System.currentTimeMillis())
          val json = gson.toJson(map)
          ctx.collect(json)
        }
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {
      flag = false

    }
  }


  class DyProcessWindowFunction() extends ProcessWindowFunction[(DataEntity, Command), String, String, TimeWindow] {

    val logger = LoggerFactory.getLogger("DyProcessWindowFunction")
    var gson: Gson = _


    override def open(parameters: Configuration): Unit = {
      gson = new Gson()
    }

    override def process(key: String, context: Context, elements: Iterable[(DataEntity, Command)], out: Collector[String]): Unit = {
      // start-end
      val taskId = elements.head._2.taskId
      val method = elements.head._2.method
      val targetAttr = elements.head._2.targetAttr
      val periodStartTime = context.window.getStart
      val periodEndTime = context.window.getEnd

      var value: Double = 0d
      method match {
        case "sum" =>
          value = 0d
        case "min" =>
          value = Double.MaxValue
        case "max" =>
          value = Double.MinValue
        case _ =>
          logger.warn("input method exception")
          return
      }

      val it = elements.toIterator
      while (it.hasNext) {
        val currentValue = it.next()._1.value
        method match {
          case "sum" =>
            value += currentValue
          case "count" =>
            value += 1
          case "min" =>
            if (currentValue < value) {
              value = currentValue
            }
          case "max" =>
            if (currentValue > value) {
              value = currentValue
            }
          case _ =>
        }
      }

      val sdf = new SimpleDateFormat("HH:mm:ss")
      val resultMap = new util.HashMap[String, String]
      resultMap.put("taskId", taskId)
      resultMap.put("method", method)
      resultMap.put("targetAttr", targetAttr)
      resultMap.put("periodStartTime", sdf.format(periodStartTime))
      resultMap.put("periodEndTime", sdf.format(periodEndTime))
      resultMap.put("value", value.toString)

      out.collect(gson.toJson(resultMap))

    }

  }

  case class Command(taskId: String, targetAttr: String, method: String, periodUnit: String, periodLength: Long, startTime: Long)
  case class DataEntity(attr: String, value: Int, time: Long)

}
