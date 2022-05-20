package com.bigdata.common.utils

import com.turing.common.PropertiesUtils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @descr RedisUtils
 *
 * @author lj.michale
 * @date 2022-05-20
 */
object RedisUtils {

  var jedisPool:JedisPool=null

  def getJedisClient: Jedis = {

    if(jedisPool == null) {
      // 开起一个连接池
      val config =  PropertiesUtils.getProperties("spark-dev.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)   //最大空闲
      jedisPoolConfig.setMinIdle(20)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool=new JedisPool(jedisPoolConfig, host, port.toInt)
    }

    jedisPool.getResource

  }

}