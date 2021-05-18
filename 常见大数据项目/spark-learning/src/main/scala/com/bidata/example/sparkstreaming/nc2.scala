package com.bidata.example.sparkstreaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object nc2 {

  def main(args: Array[String]): Unit = {
    //spark配置
    if (args.length < 1) {
      println("需要1个参数： hadoop_file 标志程序退出的文件")
      System.exit(1)
    }

    val hadoop_file = args(0) //val hadoop_file="hdfs://192.168.226.128/stop";
    val conf = new SparkConf()
    //    conf.setMaster("local[*]")
    conf.setAppName("测试spark-streaming的退出")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true") //等待数据处理完后，才停止任务，以免数据丢失

    //流配置
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext
    val ds = ssc.socketTextStream("127.0.0.1", 8888, StorageLevel.MEMORY_ONLY)
    val ds2 = ds.map((_, 1)).reduceByKey(_ + _)
    ds2.print()

    //启动
    ssc.start()
    stopByMarkFile(ssc, hadoop_file)
  }

  def stopByMarkFile(ssc:StreamingContext, hadoop_file:String):Unit= {
    val intervalMills = 3 * 1000 // 每隔3秒扫描一次消息是否存在
    var isStop = false
    //  val hadoop_master = "hdfs://localhost:9000/stop"//判断消息文件是否存在，如果存在就
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExistsMarkFile(hadoop_file)) {
        println("1 秒后开始关闭sparstreaming程序.....")
        Thread.sleep(1000)
        ssc.stop(true, true)
        delHdfsFile(hadoop_file)
      } else {
        println("***********未检测到有停止信号*****************")
      }
    }

    def isExistsMarkFile(hdfs_file_path:String):Boolean={
      val conf = new Configuration()
      val path=new Path(hdfs_file_path)
      val fs =path.getFileSystem(conf)
      fs.exists(path)
    }
    def delHdfsFile(hdfs_file_path:String):Unit={
      val conf = new Configuration()
      val path=new Path(hdfs_file_path)
      val fs =path.getFileSystem(conf)
      if (fs.exists(path)) fs.delete(path,true)
    }
  }

}
