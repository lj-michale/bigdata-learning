package com.luoj.task.learn.matric.example001

import com.alibaba.fastjson.JSON


class ParseDeserialization extends AbsDeserialization[RawData] {

  override def deserialize(message: Array[Byte]): RawData = {
    try {
      val msg = new String(message)
      val rawData = JSON.parseObject(msg, classOf[RawData])
      normalDataNum.inc() //正常数据指标
      rawData
    } catch {
      case e:Exception=>{
        dirtyDataNum.inc()   //脏数据指标
        null
      }
    }
  }
}

