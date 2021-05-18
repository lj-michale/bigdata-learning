package com.bidata.example.sparkstreaming


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

class MonitorStop(ssc: StreamingContext) extends Runnable {

  override def run(): Unit = {

    val configuration = new Configuration()
    configuration.set("fs.defaultFS","hdfs://node01:8020")
    configuration.set("user","hadoop")
    val fs: FileSystem = FileSystem.get(configuration )

    while (true) {
      try
        Thread.sleep(5000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
      val state: StreamingContextState = ssc.getState

      val bool: Boolean = fs.exists(new Path("/stopSparkTest"))

      if (bool) {
        if (state == StreamingContextState.ACTIVE) {
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}
