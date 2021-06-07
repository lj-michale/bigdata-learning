package com.jiguang.common;


import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * @author lj.michale
 * @description
 * @date 2021-06-07
 */
public class Spark {

    private SparkConf conf;

    private JavaStreamingContext jssc;

    private String bootStrapServers;

    private String topics;

    private String logLevel;


    public Spark(SparkConf conf, String bootStrapServers, String topics, String logLevel) {
        this.conf = conf;
        this.jssc = new JavaStreamingContext(conf, Durations.milliseconds(500));
        jssc.sparkContext().setLogLevel(this.setLogLevel(logLevel));
        this.bootStrapServers = bootStrapServers;
        this.topics = topics;
    }

    /**
     * 获取sparkstreamingContenxt
     * @return
     */
    public JavaInputDStream<ConsumerRecord<String, String>> getStream() {

        if (null != conf && null != bootStrapServers && null != topics) {
            // kafka配置
            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", bootStrapServers);
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);

            String[] topicArr = topics.split(",");
            Collection<String> topics = Arrays.asList(topicArr);

            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
                    .createDirectStream(jssc, LocationStrategies
                            .PreferConsistent(), ConsumerStrategies
                            .<String, String> Subscribe(topics, kafkaParams));

            return stream;

        } else {

            return null;
        }

    }

    public void streamingStart(){
        if (null != jssc) {
            jssc.start();
        }
    }

    public void streamingAwaitTermination() throws InterruptedException{
        if (null != jssc) {
            jssc.awaitTermination();
        }
    }

    public SparkConf getConf() {
        return conf;
    }

    public void setConf(SparkConf conf) {
        this.conf = conf;
    }

    public JavaStreamingContext getJssc() {
        return jssc;
    }

    public void setJssc(JavaStreamingContext jssc) {
        this.jssc = jssc;
    }

    public String getBootStrapServers() {
        return bootStrapServers;
    }

    public void setBootStrapServers(String bootStrapServers) {
        this.bootStrapServers = bootStrapServers;
    }

    public String getTopics() {
        return topics;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public String setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return logLevel;
    }

}
