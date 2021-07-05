package com.luoj.task.learn.connector.jcache;

import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
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

    private JcacheTemplate jcacheTemplate;

    private Integer batchSize = 50;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(Tuple4<String, String, Long, Integer> value, Context context) throws Exception {

        String iceLocator = ExampleConstant.JCACHE_ICE_LOCATOR;
        String proxyName = ExampleConstant.JCACHE_PROXY_NAME;
        String resourceName = ExampleConstant.JCACHE_CLUSTER_RESOURCE_NAME;

        // Write to JCache
        JcacheTemplate jcacheTemplate = new JcacheTemplate(iceLocator, proxyName, resourceName);
        PipelineTemplate pipeline = jcacheTemplate.pipelined();

        Integer initSize = 0;
        String cacheKey = value.f0;
        String uid = value.f1;
        long amount = value.f3 * (-1);
        int expire = 23273244;

//        Long slong = jcache.hdecrBy("jiguang0001", "lllllll", -2000000);
        log.info(">>>>>>>>>>>>>>>> cacheKey:{}, uid:{}, amount:{}", cacheKey, uid, amount);
        pipeline.hincrBy(cacheKey, String.valueOf(uid.toString()), amount);
        // pipeline.hdecrBy(cacheKey, String.valueOf(uid.toString()), amount);
        initSize ++;
        if (initSize % batchSize == 0) {
            log.info("===============initSize:{}", initSize);
            pipeline.sync();
//            jcacheTemplate.closeIceContext();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}