package com.luoj.task.learn.sink;

import cn.jpush.jcache.client.JcacheTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-30
 */
@Slf4j
public class SinkToCacheCloud extends RichSinkFunction<Tuple4<String,String,Long,Integer>> {

    private String jcacheLocator = "RedisClusterTest";
    private String jcacheProxy = "jCacheProxy";
    private String jcacheResourceName = "jCacheIceGrid/Locator:tcp -h 172.17.8.17 -p 4061";
    private transient JcacheTemplate jcacheTemplate;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.jcacheTemplate = new JcacheTemplate(jcacheLocator, jcacheProxy, jcacheResourceName);
    }

    /**
     * @descr
     * @param input
     * @param context
     */
    @Override
    public void invoke(Tuple4<String, String, Long, Integer> input, Context context) throws Exception {
        String key = input.f0;
        String field = input.f1;
        Long value = input.f2;
        Integer expire = input.f3;
        if (value != 0) {
            try {
                jcacheTemplate.hdecrBy(key, field, value);
            } catch (Exception e) {
                log.info("cachecloud数据插入失败");
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
       jcacheTemplate.closeIceContext();
    }
}
