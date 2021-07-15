package com.luoj.task.example.jcache;

import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
@Slf4j
public class SinkToJCache extends RichSinkFunction<Tuple4<String,String,Long,Integer>> implements Serializable {

    private String iceLocator;
    private String proxyName;
    private String resourceName;
    /**key+field的值的过期时间-3天*/
    private int expire = 259200;
    private JcacheTemplate jcacheTemplate;
    private PipelineTemplate pipeline;
    private Integer batchSize = 50;
    private boolean isRunning =true;

    public SinkToJCache(String iceLocator, String proxyName, String resourceName) {
        this.iceLocator = iceLocator;
        this.proxyName = proxyName;
        this.resourceName = resourceName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.jcacheTemplate = new JcacheTemplate(iceLocator, proxyName, resourceName);
        this.pipeline = jcacheTemplate.pipelined();
    }

    @Override
    public void invoke(Tuple4<String, String, Long, Integer> value, Context context) throws Exception {

        String cacheKey = "aurora";
        String uid = value.f1;
        long amount = value.f3;

        while (isRunning) {
            try {
                log.info(">>>>>>>>>>>>>>>> cacheKey:{}, uid:{}, amount:{}", cacheKey, uid, amount);
                pipeline.hincrBy(cacheKey, uid, amount);
                pipeline.sync();
                // 设置过期时间
                pipeline.expire(cacheKey + uid, expire);
            } catch (Exception e) {
                log.info("JCache写入失败");
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        while (jcacheTemplate != null) {
            jcacheTemplate.closeIceContext();
        }
    }

}