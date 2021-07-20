package com.luoj.task.learn.sink;

import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.isNull;

/**
 * @description  基于Flink Exactly-Once实现数据写入CacheCloud精准一次性写入
 *
 * @author lj.michale
 * @date 2021-06-30
 */
@Slf4j
public class SinkToCacheCloudByExactlyOnce extends TwoPhaseCommitSinkFunction<Tuple4<String,String,Long,Integer>, JcacheTemplate, Void> {

    private String jcacheResourceName = "jCacheIceGrid/Locator:tcp -h 172.17.8.17 -p 4061";
    private String jcacheProxy = "jCacheProxy";
    private String jcacheLocator = "RedisClusterTest";
    private int batchSize = 100;
    private long idx = 0;

    public SinkToCacheCloudByExactlyOnce(TypeSerializer<JcacheTemplate> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(new KryoSerializer<>(JcacheTemplate.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected JcacheTemplate beginTransaction() throws Exception {
        JcacheTemplate jcache = new JcacheTemplate(jcacheResourceName, jcacheProxy, jcacheLocator);
        return jcache;
    }

    @Override
    protected void invoke(JcacheTemplate jcache, Tuple4<String, String, Long, Integer> input, Context context) throws Exception {
        try {
            if (checkParam()) {
                String key = input.f0;
                String field = input.f1;
                Long value = input.f2;
                Integer expire = input.f3;
                pipelinehWrite(jcache, key, field, value);
            }
        } catch (Exception e) {
            log.info(">>>>>>>>>写入CacheCloud失败...");
            e.printStackTrace();
        }
    }

    @Override
    protected void preCommit(JcacheTemplate jcacheTemplate) throws Exception {

    }

    @Override
    protected void commit(JcacheTemplate jcacheTemplate) {

    }

    @Override
    protected void abort(JcacheTemplate jcacheTemplate) {

    }

    /**
     * @descr 非空检查
     */
    public boolean checkParam(){
        return isNull(jcacheProxy) &&isNull(jcacheLocator) && isNull(jcacheResourceName);
    }

    /**
     * CacheCloud Pipeline Batch 按批写入CacheCloud
     */
    public void pipelineBatchWrite(JcacheTemplate jcache, Map<String,String> dataMap){
        PipelineTemplate pipeline = jcache.pipelined();
        if (dataMap!=null && dataMap.size()>0) {
            Iterator<Map.Entry<String, String>> iterator = dataMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                pipeline.set(next.getKey(),next.getValue());
                idx++;
                if (idx % batchSize==0){
                    // 每100个数据批量提交一次
                    pipeline.sync();
                }
            }
        }
        jcache.closeIceContext();
    }

    /**
     * 逐行提交 k- > v
     */
    public void pipelinehWrite(JcacheTemplate jcache, String key, String field, Long value){
        if (StringUtils.isNotBlank(key)) {
            jcache.hincrBy(key, field, value);
        }
    }
}
