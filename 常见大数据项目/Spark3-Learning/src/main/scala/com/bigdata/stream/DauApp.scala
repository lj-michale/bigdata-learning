package com.bigdata.stream

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.kafka.{KafkaOffsetManagerUtils, SparkKafkaUtils}
import com.bigdata.redis.RedisUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @descr SparkStreaming从Redis中读取偏移量,实现exactly-once
 *
 * @author lj.michale
 * @date 2022-04-21
 */
object DauApp {

  case class DauInfo(mid:String, uid:String, ar:String, ch:String, vc:String,dt:String, hr:String, mi:String, ts:Long)

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc:StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic="turing-test"
    val groupId="TURING_DAU_CONSUMER"

    // 用OffsetManagerUtil.getOffset()方法，获取偏移量 -> 为达到手动提交偏移量做铺垫
    val startOffset: Map[TopicPartition, Long] = KafkaOffsetManagerUtils.getOffset(groupId, topic)

    // Kafka消费数据
    var startInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if(startOffset != null && startOffset.size > 0) {
      startInputDstream = SparkKafkaUtils.getKafkaStream(topic,
        ssc,
        startOffset,
        groupId)
    } else {
      startInputDstream  = SparkKafkaUtils.getKafkaStream(topic,ssc,groupId)
    }

    // 获得本批次偏移量的移动后的新位置
    var startupOffsetRanges: Array[OffsetRange] = null
    val startupInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = startInputDstream
      .transform { rdd =>
        startupOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 将数据转换为标准JSON格式, {"mid":"mid_7","uid":"297","ar":"42000","ch":"Xiaomi","vc":"v2.1.134","dt":"2020-05-13","hr":"09","mi":"48","ts":1650509224238}
    val startJsonObjDstream: DStream[JSONObject] = startupInputGetOffsetDstream
      .map { record =>
      val jsonString: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(jsonString)
      jSONObject
    }

    // Redis去重
    val startJsonObjWithDauDstream: DStream[JSONObject] = startJsonObjDstream
      .mapPartitions { jsonObjItr =>
      val jedis = RedisUtils.getJedisClient
      // 转换成一个JSONObject List
      val jsonObjList: List[JSONObject] = jsonObjItr.toList
      println("过滤前：" + jsonObjList.size)

      // 存储过滤后的BufferList
      val jsonObjFilteredList = new ListBuffer[JSONObject]()
      for (jsonObj <- jsonObjList) {
        // 获取日志中的ts字段的时间戳进行格式化日期
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date(jsonObj.getLong("ts")))
        // 定义每日的key，不是单纯的日期。 -> dau:2020-05-12
        val dauKey = "dau:" + dateStr
        // 获取日志中的mid
        val mid: String = jsonObj.getString("mid")
        // 用Redis的插入写操作，返回一个 0 或 1的值
        val isFirstFlag: lang.Long = jedis.sadd(dauKey, mid)
        // 数值为1 表示首次插入成功， 数值为0 表示插入失败，以此达到去重的目的
        if(isFirstFlag==1L) {
          jsonObjFilteredList+=jsonObj
        }
      }
      jedis.close()
      println("过滤后："+jsonObjFilteredList.size)
      // 返回一个去重的List集合
      jsonObjFilteredList.toIterator
    }

    // 变换结构
    val dauInfoDstream: DStream[DauInfo] = startJsonObjWithDauDstream.map { jsonObj =>
      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(jsonObj.getLong("ts")))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      val dt: String = dateTimeArr(0)
      val timeArr: Array[String] = dateTimeArr(1).split(":")
      val hr = timeArr(0)
      val mi = timeArr(1)
      DauInfo(
        jsonObj.getString("mid"),
        jsonObj.getString("uid"),
        jsonObj.getString("ar"),
        jsonObj.getString("ch"),
        jsonObj.getString("vc"),
        dt,
        hr,
        mi,
        jsonObj.getLong("ts")
      )
    }

    //要插入gmall1122_dau_info_2020xxxxxx  索引中
    dauInfoDstream.foreachRDD {rdd=>
      // 又区内遍历
      rdd.foreachPartition { dauInfoItr =>
        // 封装程一个以mid和DauInfo的对偶元组的List
        val dataList: List[(String, DauInfo)] = dauInfoItr.toList.map { dauInfo => (dauInfo.mid, dauInfo) }
        // 获取时间
        val dt = new SimpleDateFormat("yyyyMMdd").format(new Date())
        // 建立index gmall1122_dau_info_20200513
        // ES查找 ： GET gmall1122_dau_info_20200513/_search
        val indexName = "gmall1122_dau_info_" + dt
        //   MyEsUtil.saveBulk(dataList, indexName)
      }

      KafkaOffsetManagerUtils.saveOffset(groupId,topic,startupOffsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
