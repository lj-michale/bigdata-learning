package com.luoj.task.learn.sink.example001;

import cn.jpush.jcache.client.JcacheTemplate;
import com.luoj.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import static com.luoj.common.GenerateRandomDataUtils.*;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
@Slf4j
public class JCacheSinkExample001 {



    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = createParameterTool(args);

        // 1. 初始化流式运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Get Data And Do Some ETL
        SingleOutputStreamOperator<Tuple4<String, String, Long, Integer>> dStream =
                env.addSource(new RichSourceFunction<Tuple4<String, String, Long, Integer>>() {

            private boolean isCanaled = false;

            @Override
            public void run(SourceContext<Tuple4<String, String, Long, Integer>> ctx) throws Exception {
                while (!isCanaled) {
                    long  currentTimeStamp = System.currentTimeMillis();
                    ctx.collect(new Tuple4<>(getRandomUserID(), getRandomUserName(), currentTimeStamp, getRandomPrice().intValue()));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCanaled = true;
            }
        }).filter((FilterFunction<Tuple4<String, String, Long, Integer>>) value -> value != null);

        String jcacheLocator = parameterTool.get("jcacheLocator");
        String jcacheProxy = parameterTool.get("jcacheProxy");
        String jcacheResourceName = parameterTool.get("jcacheResourceName");

        // Write to JCache
        JcacheTemplate jcacheTemplate = new JcacheTemplate(jcacheLocator, jcacheProxy, jcacheResourceName);
        dStream.addSink(new SinkToJCache(jcacheTemplate));

    }

    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        return ParameterTool
                .fromPropertiesFile(JCacheSinkExample001.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());
    }

}
