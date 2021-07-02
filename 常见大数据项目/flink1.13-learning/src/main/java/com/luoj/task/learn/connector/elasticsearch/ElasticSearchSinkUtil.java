package com.luoj.task.learn.connector.elasticsearch;




import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.util.List;
import java.util.Map;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-02
 */
public class ElasticSearchSinkUtil {

    /**
     * es sink
     *
     * @param <T>
     * @param hosts es hosts
     * @param bulkFlushMaxActions bulk flush size
     * @param parallelism 并行数
     * @param data 数据
     * @param func
     * @param parameterTool
     */
    public static <T> void addSink(Map<String, String> config,
                                   List<HttpHost> hosts,
                                   int bulkFlushMaxActions,
                                   int parallelism,
                                   SingleOutputStreamOperator<T> data,
                                   ElasticsearchSinkFunction<T> func, ParameterTool parameterTool) {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

}
