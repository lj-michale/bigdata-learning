package com.luoj.task.learn.connector.jcache;

import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
import cn.jpush.jcache.vo.Response;
import com.luoj.common.ExampleConstant;
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
public class SinkToJCache extends RichSinkFunction<Tuple4<String,String,Long,Integer>>  implements java.io.Serializable{

    String iceLocator = ExampleConstant.JCACHE_ICE_LOCATOR;
    String proxyName = ExampleConstant.JCACHE_PROXY_NAME;
    String resourceName = ExampleConstant.JCACHE_CLUSTER_RESOURCE_NAME;

    private JcacheTemplate jcacheTemplate;
    private PipelineTemplate pipeline;

    private Integer batchSize = 50;

    @Override
    public void open(Configuration parameters) throws Exception {
        // Write to JCache
        this.jcacheTemplate = new JcacheTemplate(iceLocator, proxyName, resourceName);
        this.pipeline = jcacheTemplate.pipelined();
    }

    @Override
    public void invoke(Tuple4<String, String, Long, Integer> value, Context context) throws Exception {

        Integer initSize = 0;
        String cacheKey = "LLLLLLLLLLLLLLLLLLLLLLLLLL";
        String uid = value.f1;
        long amount = value.f3;
        int expire = 23273244;

        try {
            log.info(">>>>>>>>>>>>>>>> cacheKey:{}, uid:{}, amount:{}", cacheKey, uid, amount);
            pipeline.hincrBy(cacheKey, uid, amount);
            pipeline.sync();
        } catch (Exception e) {
            log.info("JCache写入失败");
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        jcacheTemplate.closeIceContext();
    }

}