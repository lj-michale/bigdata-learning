package com.bigdata.common.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
 * @descr KafkaOffsetManagerUtils
 *
 * @author lj.michale
 * @date 2022-05-20
 */
object KafkaOffsetManagerUtils {

  /**
   * 从Redis中读取偏移量
   *
   * @param groupId
   * @param topic
   * @return
   */
  def getOffset(groupId: String,
                topic: String): Map[TopicPartition, Long] = {
    var offsetMap = Map[TopicPartition, Long]()
    val jedisClient: Jedis = RedisUtils.getJedisClient
    val redisOffsetMap: scala.collection.Map[String, String] = jedisClient.hgetAll("offset:" + groupId + ":" + topic)
    jedisClient.close()

    if(redisOffsetMap != null && redisOffsetMap.isEmpty) {
      null
    } else {
      val redisOffsetList: List[(String, String)] = redisOffsetMap.toList
      val kafkaOffsetList: List[(TopicPartition, Long)] = redisOffsetList.map { case ( partition, offset) =>
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
      kafkaOffsetList.toMap
    }

  }


  /**
   * 偏移量写入到Redis中
   *
   * @param groupId
   * @param topic
   * @param offsetArray
   */
  def saveOffset(groupId:String,
                 topic:String,
                 offsetArray:Array[OffsetRange]): Unit = {

    if (offsetArray != null && offsetArray.size > 0) {
      val offsetMap: Map[String, String] = offsetArray.map { offsetRange =>
        val partition: Int = offsetRange.partition
        val untilOffset: Long = offsetRange.untilOffset
        (partition.toString, untilOffset.toString)
      }.toMap

      val jedisClient: Jedis = RedisUtils.getJedisClient
      jedisClient.hmset("offset:" + groupId + ":" + topic, offsetMap)
      jedisClient.close()
    }

  }

}
