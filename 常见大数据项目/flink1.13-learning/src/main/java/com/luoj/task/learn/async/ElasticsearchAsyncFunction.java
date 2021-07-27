package com.luoj.task.learn.async;

import avro.shaded.com.google.common.cache.CacheBuilder;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-26
 */
public class ElasticsearchAsyncFunction extends RichAsyncFunction<Tuple4<String, String, String, Integer>, Tuple5<String, String, String, Integer, Integer>> {

    private String host;
    private Integer port;
    private String user;
    private String password;
    private String index;
    private String type;

    public ElasticsearchAsyncFunction(String host, Integer port, String user, String password, String index, String type) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.index = index;
        this.type = type;
    }

    private RestHighLevelClient restHighLevelClient;
    private Cache<String, Integer> cache;
    /**
     * 和ES建立连接
     *
     * @param parameters
     */
    @Override
    public void open(Configuration parameters) {
        //ES Client
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
        restHighLevelClient = new RestHighLevelClient(
                RestClient
                        .builder(new HttpHost(host, port))
                        .setHttpClientConfigCallback(httpAsyncClientBuilder -> HttpAsyncClientBuilder.create()));

        //初始化缓存
        cache = (Cache<String, Integer>) CacheBuilder.newBuilder().maximumSize(2).expireAfterAccess(5, TimeUnit.MINUTES).build();
    }

    /**
     * 关闭连接
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        restHighLevelClient.close();
    }

    /**
     * 异步调用
     *
     * @param input
     * @param resultFuture
     */
    @Override
    public void asyncInvoke(Tuple4<String, String, String, Integer> input, ResultFuture<Tuple5<String, String, String, Integer, Integer>> resultFuture) {

        // 1、先从缓存中取
        Integer cachedValue = cache.getIfPresent(input.f0);
        if (cachedValue != null) {
            System.out.println("从缓存中获取到维度数据: key=" + input.f0 + ",value=" + cachedValue);
            resultFuture.complete(Collections.singleton(new Tuple5<>(input.f0, input.f1, input.f2, input.f3, cachedValue)));

            // 2、缓存中没有,则从外部存储获取
        } else {
            searchFromES(input, resultFuture);
        }
    }

    /**
     * 当缓存中没有数据时，从外部存储ES中获取
     *
     * @param input
     * @param resultFuture
     */
    private void searchFromES(Tuple4<String, String, String, Integer> input, ResultFuture<Tuple5<String, String, String, Integer, Integer>> resultFuture) {

        // 1、构造输出对象
        Tuple5<String, String, String, Integer, Integer> output = new Tuple5<>();
        output.f0 = input.f0;
        output.f1 = input.f1;
        output.f2 = input.f2;
        output.f3 = input.f3;

        // 2、待查询的Key
        String dimKey = input.f0;

        // 3、构造Ids Query
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.types(type);
        searchRequest.source(SearchSourceBuilder.searchSource().query(QueryBuilders.idsQuery().addIds(dimKey)));

        RequestOptions requestOptions = RequestOptions.DEFAULT;
        // 4、用异步客户端查询数据
        restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {

            //成功响应时处理
            @Override
            public void onResponse(SearchResponse searchResponse) {
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits.length > 0) {
                    JSONObject obj = JSON.parseObject(searchHits[0].getSourceAsString());
                    Integer dimValue = obj.getInteger("age");
                    output.f4 = dimValue;
                    cache.put(dimKey, dimValue);
                    System.out.println("将维度数据放入缓存: key=" + dimKey + ",value=" + dimValue);
                }
                resultFuture.complete(Collections.singleton(output));
            }

            //响应失败时处理
            @Override
            public void onFailure(Exception e) {
                output.f4 = null;
                resultFuture.complete(Collections.singleton(output));
            }
        });

    }

    /**超时时处理*/
    @Override
    public void timeout(Tuple4<String, String, String, Integer> input, ResultFuture<Tuple5<String, String, String, Integer, Integer>> resultFuture) {
        searchFromES(input, resultFuture);
    }
}
