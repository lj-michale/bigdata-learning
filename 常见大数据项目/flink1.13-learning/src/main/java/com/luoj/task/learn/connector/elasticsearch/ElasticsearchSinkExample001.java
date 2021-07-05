package com.luoj.task.learn.connector.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.luoj.common.ESSinkUtil;
import com.luoj.common.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.luoj.common.GenerateRandomDataUtils.*;
import static com.luoj.common.PropertiesConstants.*;


/**
 * @author lj.michale
 * @description  Flink写入ES Example
 * 参考资料：
 * @date 2021-07-02
 */
@Slf4j
public class ElasticsearchSinkExample001 {


    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        // 1. 初始化流式运行环境
        Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.13-learning\\checkpoint");


        // 3. 定义数据源，每秒发送一个hadoop单词
        SingleOutputStreamOperator<String> wordDSWithWaterMark = env.addSource(new RichSourceFunction<Tuple6<String, String, String, String, Double, Long>>() {

            private boolean isCanaled = false;

            @Override
            public void run(SourceContext<Tuple6<String, String, String, String, Double, Long>> ctx) throws Exception {
                while (!isCanaled) {
                    long  currentTimeStamp = System.currentTimeMillis();
                    ctx.collect(Tuple6.of(getRandomUserID(), getRandomUserName(), getRandomProductName(), getRandomProductID(), getRandomPrice(), currentTimeStamp));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isCanaled = true;
            }
        }).map(new MapFunction<Tuple6<String, String, String, String, Double, Long>, String>() {
            @Override
            public String map(Tuple6<String, String, String, String, Double, Long> input) throws Exception {
                String str = input.f0 + "," + input.f1 + "," +  input.f2 + "," +  input.f3 + "," +  input.f4.toString() + "," +  input.f5.toString();
                return str;
            }
        });

        wordDSWithWaterMark.print();


        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "my-cluster-name");
        // 该配置表示批量写入ES时的记录条数
        config.put("bulk.flush.max.actions", "200");

        //1、用来表示是否开启重试机制
        config.put("bulk.flush.backoff.enable", "true");
        //2、重试策略，又可以分为以下两种类型
        //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
        config.put("bulk.flush.backoff.type", "EXPONENTIAL");
        //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
        config.put("bulk.flush.backoff.type", "CONSTANT");
        //3、进行重试的时间间隔。对于指数型则表示起始的基数
        config.put("bulk.flush.backoff.delay", "2");
        //4、失败重试的次数
        config.put("bulk.flush.backoff.retries", "3");
        // 其他配置
        // bulk.flush.max.actions: 批量写入时的最大写入条数
        // bulk.flush.max.size.mb: 批量写入时的最大数据量
        // bulk.flush.interval.ms: 批量写入的时间间隔，配置后则会按照该时间间隔严格执行，无视上面的两个批量写入配置

        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);

        ElasticSearchSinkUtil.addSink(config, esAddresses, bulkSize, sinkParallelism, wordDSWithWaterMark, new ElasticsearchSinkFunction<String>() {
            /**
             * @descr es 更新操作
             * @param s
             */
            public UpdateRequest updateIndexRequest (String s) throws IOException {
                JSONObject object = JSON.parseObject(s);
                String id = object.getJSONObject("esConfig").get("id").toString();
                UpdateRequest updateRequest=new UpdateRequest();
                updateRequest.index("zjf_2020-05-27").type("doc").id(id)
                        .doc(XContentFactory.jsonBuilder().startObject().field("source",s).endObject())
                        .upsert(s, XContentType.JSON);
                return updateRequest;
            };

            ///// ES更新操作
//            @Override
//            public void process(String element, RuntimeContext runtimeContext, RequestIndexer indexer) {
//                try {
//                    indexer.add(updateIndexRequest(element));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }

            ///// ES插入操作
            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(Requests.indexRequest()
                        .index("zjf_2020-05-26")
                        .type("ooo")
//                        .source
                        .source(JSON.parseObject(element)));
            }

        }, parameterTool);


        log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);

        env.execute("ElasticsearchSinkExample001");

    }

}
