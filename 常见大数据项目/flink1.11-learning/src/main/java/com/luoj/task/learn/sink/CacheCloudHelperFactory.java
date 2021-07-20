package com.luoj.task.learn.sink;

import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;

/**
 * @author lj.michale
 * @description 将数据写入CacheCloud
 * {"iceLocator":"jCacheIceGrid/Locator:tcp -h 172.17.8.17 -p 4061","proxyName":"jCacheProxy","resourceName":"RedisClusterTest"}
 * @date 2021-06-30
 */
@Slf4j
public class CacheCloudHelperFactory {

//    private JedisFactory jedisFactory;
//    private RedisFactory redisFactory;

    private JcacheTemplate jcache;

    /**
     * CacheCloud种 key的过期时间
     */
    private Integer expire = 259200;

    /**
     * 172.17.8.101:9094,172.17.8.89:9094,172.17.8.48:9094
     */
    private String server="172.17.8.101:9094,172.17.8.89:9094,172.17.8.48:9094";

    /**
     * jCacheIceGrid/Locator:tcp -h 172.17.8.17 -p 4061
     */
    private String iceLocator="jCacheIceGrid/Locator:tcp -h 172.17.8.17 -p 4061";

    /**
     * jCacheProxy
     */
    private String proxyName="jCacheProxy";

    /**
     * RedisClusterTest
     */
    private String resourceName="RedisClusterTest";


    public CacheCloudHelperFactory(String server, String iceLocator, String proxyName, String resourceName) {
        this.server = server;
        this.iceLocator = iceLocator;
        this.proxyName = proxyName;
        this.resourceName = resourceName;
        initRedisStore();
    }


    /**
     * @descr 初始化Redis存储
     */
    private void  initRedisStore() {
        // cachecloud
        jcache = new JcacheTemplate(iceLocator,proxyName,resourceName);
    }


    /**
     * @descr 将数据插入CacheCloud
     */
    public void hincrby(String key,String field,long value,int expire) throws Exception {
        jcache.hincrBy(key, field, value);
        long ttl = jcache.ttl(key);
        if ((ttl > expire && expire > 0) || ttl == -1) {
            // 当生命周期大于过期时间和没有设置生命周期的情况下，进行设置过期时间
            jcache.expire(key, expire);
        }
    }

//    public void hincrby(Map<String, Map<String,Long>> map, int expire, boolean isDebug) throws Exception{
//        PipelineTemplate pipeline = jcache.pipelined();
//        Set<String> keys = map.keySet();
//        for (String key : keys) {
//            if (isDebug) {
//                log.info("the write hincrby redis key:" + key + ",size:" + map.get(key).size());
//            }
//            Map<String, Long> results = map.get(key);
//            for (Map.Entry<String, Long> entry : results.entrySet()) {
//                pipeline.hincrBy(key, entry.getKey(), entry.getValue());
//            }
//            pipeline.expire(key, expire);
//        }
//        pipeline.sync();
//    }

    public String get(String key) throws Exception{
        String result = jcache.get(key);
        return result;
    }

    public void expire(String key,int expire) throws Exception{
        jcache.expire(key,expire);
    }




}
