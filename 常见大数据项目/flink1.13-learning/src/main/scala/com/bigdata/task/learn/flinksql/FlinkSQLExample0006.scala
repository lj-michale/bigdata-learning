package com.bigdata.task.learn.flinksql

import java.time.ZoneId
import java.util.Calendar

import com.bigdata.task.learn.tablesql.LateTest.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.types.Row

import scala.collection.immutable
import scala.util.Random

/**
 * @description Converting between DataStream and Table
 *  https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/data_stream_api/
 * @author lj.michale
 * @date 2021-07-16
 */
object FlinkSQLExample0006 {

  def main(args: Array[String]): Unit = {

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))
    // 状态后端-HashMapStateBackend
    env.setStateBackend(new HashMapStateBackend)
    //等价于MemoryStateBackend
    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint")

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

    import org.apache.flink.api.scala._
    val streamDS: DataStream[(String, String)] = env.addSource(new SensorSource)

    import org.apache.flink.table.api._
    // 声明一个额外的字段作为时间属性字段
    val table:Table = tableEnv.fromDataStream(streamDS, $"user_name", $"data", $"user_action_time".proctime)
    val windowedTable:GroupWindowedTable = table.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")

    /**
     * @descr Converting between DataStream and Table
     */
    /////////////////////////////////////////////////////////  DataStream1
    // create a DataStream// create a DataStream
    val dataStream = env.fromElements("Alice", "Bob", "John")
    // interpret the insert-only DataStream as a Table
    val inputTable = tableEnv.fromDataStream(dataStream)
    // register the Table object as a view and query it
    tableEnv.createTemporaryView("InputTable", inputTable)
    val resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable")
    // interpret the insert-only Table as a DataStream again
    val resultStream = tableEnv.toDataStream(resultTable)
    // add a printing sink and execute in DataStream API
    resultStream.print()

    ///////////////////////////////////////////////////////// DataStream2
//    val dataStream2 = env.fromElements(
//      Row.of("Alice", Int.box(12)),
//      Row.of("Bob", Int.box(10)),
//      Row.of("Alice", Int.box(100))
//    )(Types.ROW(Types.STRING, Types.INT))
//    // interpret the insert-only DataStream as a Table
//    val inputTable2 = tableEnv.fromDataStream(dataStream).as("name", "score")
//    // register the Table object as a view and query it
//    // the query contains an aggregation that produces updates
//    tableEnv.createTemporaryView("InputTable", inputTable2)
//    val resultTable2= tableEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name")
//    // interpret the updating Table as a changelog DataStream
//    val resultStream2 = tableEnv.toChangelogStream(resultTable2)
//    // add a printing sink and execute in DataStream API
//    resultStream2.print()

    ///////////////////////////////////////////////////////// DataStream3




    env.execute("FlinkSQLExample0006")

  }


  // 编造自定义Source
  class SensorSource extends RichSourceFunction[(String, String)] {
    // 表示数据源是否运行正常
    var running: Boolean = true
    // 上下文参数来发送数据
    override def run(sContext:SourceFunction.SourceContext[(String, String)]) {
      val rand = new Random()
      // 使用高斯噪声产生随机温度
      val curFtemp = (1 to 10).map(
        i => ("sensor_" + i, rand.nextGaussian() * 20)
      )
      // 产生无限流数据
      while(running){
        val mapTemp:immutable.IndexedSeq[(String,String)] = curFtemp.map(
          t => (t._1, (t._2 + (rand.nextGaussian()*10)).toString)
        )
        // 发送出去
        mapTemp.foreach(t => sContext.collect(t._1,t._2))
        // 每隔100ms发送一条传感器数据
        Thread.sleep(100)
      }
    }
    override def cancel(): Unit = running =false
  }

}
