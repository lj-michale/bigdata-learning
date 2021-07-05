package com.luoj.task.example.jcache;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
public class JCacheSinkUtil {

    /**
     * @descr Sink to JCache
     * @param jcacheLocator
     * @param jcacheProxy
     * @param jcacheResourceName
     * @param dataStream
     */
    public static <T> void addSink(String jcacheLocator,
                                   String jcacheProxy,
                                   String jcacheResourceName,
                                   SingleOutputStreamOperator<T> dataStream) {



    }


}
