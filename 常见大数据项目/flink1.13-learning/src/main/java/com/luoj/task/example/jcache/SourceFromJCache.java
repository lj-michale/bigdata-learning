package com.luoj.task.example.jcache;

import cn.jpush.jcache.client.AsyncJcacheTemplate;
import cn.jpush.jcache.client.JcacheTemplate;
import cn.jpush.jcache.client.PipelineTemplate;
import cn.jpush.jcache.vo.Response;
import com.luoj.common.ExampleConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.Serializable;
import java.util.Map;

/**
 * @author lj.michale
 * @description  Flink自定义Source：SourceFunction、ParallelSourceFunction、RichParallelSourceFunction
 * @date 2021-07-08
 */
@Slf4j
public class SourceFromJCache extends RichSourceFunction<String> {

    private String iceLocator;
    private String proxyName;
    private String resourceName;
    private JcacheTemplate jcacheTemplate;
    private PipelineTemplate pipeline;
    private AsyncJcacheTemplate asyncJcacheTemplate;
    private boolean isRunning =true;
    private final long SLEEP_MILLION=5000;

    public SourceFromJCache(String iceLocator, String proxyName, String resourceName) {
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
    public void run(SourceContext<String> sc) throws Exception {
        String reValue = "";
        while (isRunning) {
            try {
                Map<String, String> sourceMap = jcacheTemplate.hgetAll("aurora");
                for(Map.Entry<String, String> aurora : sourceMap.entrySet()) {
                    String key = aurora.getKey();
                    String value = aurora.getValue();
                    System.out.println("key:"+key+"，--value："+value);
                    reValue = key + value;
                    sc.collect(reValue);
                }
            } catch (Exception e) {
                log.error("Jcache连接报错");
                e.printStackTrace();
            }
        }
    }


    @Override
    public void cancel() {
         isRunning = false;
         while (jcacheTemplate != null) {
             jcacheTemplate.closeIceContext();
         }
    }
}
