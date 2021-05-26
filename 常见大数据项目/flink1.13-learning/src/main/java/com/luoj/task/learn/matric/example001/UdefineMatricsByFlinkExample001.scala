package com.luoj.task.learn.matric.example001

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStreamSource


/**
 * @author lj.michale
 * @description User-defined Metrics
 * @date 2021-05-26
 */
object UdefineMatricsByFlinkExample001 {

  def main(args: Array[String]): Unit = {

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaPro = new Properties();
    kafkaPro.setProperty("bootstrap.servers","114.116.219.197:5008");
    kafkaPro.setProperty("group.id","KafkaFlinkTest1a");

    val consumerSource: CustomerKafkaConsumer[RawData] = new CustomerKafkaConsumer[RawData]("topicName", new ParseDeserialization, kafkaPro)

    import org.apache.flink.streaming.api.scala._
    val kafkaDataStream:DataStream[RawData] = env.addSource(consumerSource)

    kafkaDataStream.print()

    env.execute(this.getClass.getName)

  }

}
