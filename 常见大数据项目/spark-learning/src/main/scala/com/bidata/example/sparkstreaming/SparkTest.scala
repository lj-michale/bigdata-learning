package com.bidata.example.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkTest {

  def createSSC( ckPath:String): _root_.org.apache.spark.streaming.StreamingContext = {

    val update: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {
      //当前批次内容的计算
      val sum: Int = values.sum
      //取出状态信息中上一次状态
      val lastStatu: Int = status.getOrElse(0)
      Some(sum + lastStatu)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkTest")
    //	如果为true，Spark会StreamingContext在JVM关闭时正常关闭，而不是立即关闭。
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(ckPath)

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)
    val word: DStream[String] = line.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))
    val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)
    wordAndCount.print()

    ssc

  }

  def main(args: Array[String]): Unit = {

    val ckPath:String = "E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\spark-learning\\ck"

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(ckPath, () => createSSC(ckPath))
    new Thread(new MonitorStop(ssc)).start()

    ssc.start()
    ssc.awaitTermination()
  }

}
