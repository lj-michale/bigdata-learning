package com.bigdata.common

import com.alibaba.fastjson.JSON
import com.aurora.mq.AbsDeserialization
import com.bigdata.bean.RawData
import org.slf4j.{Logger, LoggerFactory}

/**
 * @description 数据解析
 *
 * @author lj.michale
 * @date 2021-08-21
 */
class ParseDeserialization extends AbsDeserialization[RawData] {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * @descr 反序列化
   * @param message
   */
  override def deserialize(message: Array[Byte]): RawData =
    try {
        // O-20210821171402-42791|C-00004|P-00004|清蒸婉鱼|176.34|1939.74|11|2021-08-21 17:14:02
        val msg:String = new String(message)
        println(s" >>>>>>>>>>> 被序列化之前的数据msg:${msg}")
        val rawData:RawData = JSON.parseObject(msg, classOf[RawData])
        println(s" >>>>>>>>>>> 被序列化之前的数据RawData:${RawData.toString()}")
        //正常数据指标
        normalDataNum.inc()
        rawData
    } catch {
      case e:Exception=>{
        //脏数据指标
        dirtyDataNum.inc()
        null
      }
    }

}