package com.luoj.task.learn.sink.example001;

import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
@Slf4j
public class SinkToJCache extends RichSinkFunction<Tuple4<String,String,Long,Integer>> {

    private JcacheTemplate jcacheTemplate;
    private Integer batchSize = 50;

    public SinkToJCache(JcacheTemplate jcacheTemplate) {
        this.jcacheTemplate = jcacheTemplate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(Tuple4<String, String, Long, Integer> value, Context context) throws Exception {

        Integer initSize = 0;
        String cacheKey = value.f0;
        String uid = value.f1;
        long amount = value.f2;
        int expire = value.f3;

        PipelineTemplate pipeline = jcacheTemplate.pipelined();
        pipeline.hdecrBy(cacheKey, "JG-1218314", amount);

        if (initSize % batchSize == 0) {
            pipeline.sync();
//            jcacheTemplate.closeIceContext();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}
