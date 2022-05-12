//package com.aurora.common.utils;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.Properties;
//
///**
// * @descri RedisUtils
// *
// * @author lj.michale
// * @date 2022-05-10
// */
//@Slf4j
//public class RedisUtils {
//
//    private  static  JedisPool jedisPool = null;
//
//    static{
//        // 读取配置文件，获得文件中的属性配置
//        Properties properties = new Properties();
//        try {
//            // 获取配置属性
//            BufferedReader bufferedReader = new BufferedReader(new FileReader("common-test.properties"));
//            properties.load(bufferedReader);
//            String host = properties.getProperty("redis.host");
//            System.out.println(host);
//            int port = Integer.parseInt(properties.getProperty("redis.port"));
//            String password = properties.getProperty("redis.password");
//            // 创建连接池配置文件对象
//            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
//            // 最大连接数
//            config.setMaxTotal(100);
//            // 最大空闲
//            config.setMinIdle(20);
//            // 最小空闲
//            config.setMaxIdle(20);
//            // 忙碌时是否需要等待
//            config.setBlockWhenExhausted(true);
//            // 忙碌时等待时长
//            config.setMaxWaitMillis(20000);
//            // 每次获取连接是进行测试
//            config.setTestOnBorrow(true);
//            // 封装连接信息
//            jedisPool = new JedisPool(config,host,port,20000,password);
//        } catch (IOException e) {
//            log.error("Redis连接异常:{}", e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    public static Jedis getConnection(){
//        return jedisPool.getResource();
//    }
//
//    public static void main(String[] args) {
//        System.out.println(jedisPool.getResource());
//        System.out.println("连接成功");
//    }
//
//}