package com.luoj.task.learn.api.processfunction

object TestKeyedBroadcastProcessFunction {

  import org.apache.flink.api.common.state.MapStateDescriptor
  import org.apache.flink.api.common.typeinfo.BasicTypeInfo
  import org.apache.flink.streaming.api.datastream.BroadcastStream
  import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
  import org.apache.flink.streaming.api.scala._
  import org.apache.flink.util.Collector

  object TestKeyedBroadcastProcessFunction extends App {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1: DataStream[String] = env.socketTextStream("localhost", 9999)
    val stream2: DataStream[String] = env.socketTextStream("localhost", 8888)


    private val StudentStream: DataStream[Student] = stream1
      .map(data => {
        val arr = data.split(",")
        Student(arr(0).toInt, arr(1), arr(2))
      })

    val descriptor = new MapStateDescriptor[String, String]("classInfo", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

    val ClassStream: DataStream[Class] = stream2.map(data => {
      val arr = data.split(",")
      Class(arr(0).toInt, arr(1))
    })
    val ClassBradoStream: BroadcastStream[Class] = ClassStream.broadcast(descriptor)

    StudentStream.keyBy(_.classId)
      .connect(ClassBradoStream)
      .process(new CustomKeyedBroadcastProcessFunction)
      .print("TestBroadcastProcessFunction")

    env.execute()
  }

  /**
   * key 类型
   * 未广播数据类型
   * 广播数据类型
   * 输出数据类型
   */
  class CustomKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String, Student, Class, String] {
    override def processElement(value: Student, ctx: KeyedBroadcastProcessFunction[String, Student, Class, String]#ReadOnlyContext, out: Collector[String]): Unit = {

      println(s"processElement.key = ${ctx.getCurrentKey}")

      val classInfo = ctx.getBroadcastState(TestKeyedBroadcastProcessFunction.descriptor)

      val className: String = classInfo.get(value.classId)

      out.collect(s"stuId:${value.id}  stuName:${value.name} stuClassName:${className}")
    }

    override def processBroadcastElement(value: Class, ctx: KeyedBroadcastProcessFunction[String, Student, Class, String]#Context, out: Collector[String]): Unit = {
      val classInfo = ctx.getBroadcastState(TestKeyedBroadcastProcessFunction.descriptor)
      println("更新状态")
      classInfo.put(value.id.toString, value.name)
    }
  }

}
