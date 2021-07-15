package com.luoj.task.example.jcache;

import cn.jpush.jcache.client.JcacheTemplate;
import com.luoj.common.ExampleConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */
@Slf4j
public class JCacheSourceExample002 {

    public static void main(String[] args) throws Exception {

        // 1. 初始化流式运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");

        String iceLocator = ExampleConstant.JCACHE_ICE_LOCATOR;
        String proxyName = ExampleConstant.JCACHE_PROXY_NAME;
        String resourceName = ExampleConstant.JCACHE_CLUSTER_RESOURCE_NAME;
//        JcacheTemplate jcacheTemplate = new JcacheTemplate(iceLocator, proxyName, resourceName);

        DataStreamSource<String> dStream = env.addSource(new SourceFromJCache(iceLocator, proxyName, resourceName));
        dStream.print();

        env.execute("JCacheSourceExample002");

    }


}
