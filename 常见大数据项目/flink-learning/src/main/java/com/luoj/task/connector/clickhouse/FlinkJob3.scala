//package com.luoj.task.connector.clickhouse
//
//import java.util
//
//import org.apache.flink.api.common.state.ValueStateDescriptor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.table.api.EnvironmentSettings
//import org.apache.flink.table.api.bridge.scala._
//import org.apache.flink.types.Row
//import org.apache.flink.util.Collector
//
//object FlinkJob3 {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
//    val tableEnv = StreamTableEnvironment.create(env, settings)
//
//    val datagen =
//      """
//        |create table datagen(
//        |name string,
//        |address string,
//        |age int
//        |)with(
//        |'connector'='datagen',
//        |'rows-per-second'='5'
//        |)
//        |""".stripMargin
//
//    tableEnv.executeSql(datagen)
//    val table = tableEnv.sqlQuery("select name,address,abs(age) from datagen")
//    val stream:DataStream[Row] = tableEnv.toAppendStream[Row](table)
//    val ports = util.ArrayList[String]
//    val tableCols = util.ArrayList[String]
//    stream.addSink(
//        new ClickHouseJDBCSinkFunction(
//          new ClickHouseJDBCOutputFormat("", "", ports , 3, 500, "report", "t_awake", tableCols)
//        )
//      ).name("clickhouse-sink");
//
//    // flink有timeWindow和countWindow 都不满足需求
//    // 我既想按照一定时间聚合，又想如果条数达到batchSize就触发计算，只能定义触发器
//    // todo 目前问题是，在这个时间窗口内达到了maxcount计算了一次，触发计算后到窗口关闭的这段时间，有一批数据，0<数据量< maxcount，这一批数据会在时间窗口到达时触发计算
//    val stream2: DataStream[util.List[Row]] = stream.timeWindowAll(Time.seconds(5))
//      .trigger(new MyCountTrigger(20))
//      .process(new MyPWFunction)
//
//
//    val sql = "INSERT INTO user2 (name, address, age) VALUES (?,?,?)"
//    val tableColums = Array("name", "address", "age")
//    val types = Array("string", "string", "int")
//    stream2.print()
//    stream2.addSink(new MyClickHouseSink3(sql, tableColums, types))
//    env.execute("clickhouse sink test")
//  }
//
//  //触发器触发或者到时间后，把所有的结果收集到了这里，在这里计算
//  class MyPWFunction extends ProcessAllWindowFunction[Row, util.List[Row], TimeWindow] {
//    override def process(context: Context, elements: Iterable[Row], out: Collector[util.List[Row]]): Unit = {
//      val list = new util.ArrayList[Row]
//      elements.foreach(x => list.add(x))
//      out.collect(list)
//    }
//  }
//
//  class MyCountTrigger(maxCount: Int) extends Trigger[Row, TimeWindow] {
//    private lazy val count: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("counter", classOf[Int])
//
//    override def onElement(element: Row, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
//      val cnt_state = ctx.getPartitionedState(count)
//      val cnt = cnt_state.value()
//      cnt_state.update(cnt + 1)
//      if ((cnt + 1) >= maxCount) {
//        cnt_state.clear()
//        TriggerResult.FIRE_AND_PURGE
//      } else {
//        TriggerResult.CONTINUE
//      }
//    }
//
//    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
//
//    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
//
//    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
//      ctx.getPartitionedState(count).clear()
//      TriggerResult.PURGE
//    }
//
//  }
//
//}
