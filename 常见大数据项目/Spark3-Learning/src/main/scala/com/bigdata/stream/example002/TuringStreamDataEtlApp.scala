package com.bigdata.stream.example002

import com.alibaba.fastjson.JSONObject
import com.bigdata.core.AbstractTuringBaseStreamApp
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object TuringStreamDataEtlApp extends AbstractTuringBaseStreamApp {

  override val master: String = "local[*]"
  override val appName: String = "TuringStreamDataEtlApp"
  override val groupId: String = "TuringStreamDataEtlApp_Consumer"
  override val topic: String = "turing-test"
  override val bachTime: Int = 5

  /**
   * @descr sparkstreaming 数据业务逻辑处理实现
   *
   * @param ssc
   * @param beginOffset
   * @param sourceStream
   */
  override def run(ssc: StreamingContext, beginOffset: Map[TopicPartition, Long],
                   sourceStream: DStream[JSONObject]): Unit = {



  }


}
